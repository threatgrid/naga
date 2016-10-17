(ns ^{:doc "A storage implementation over in-memory indexing. Includes full query engine."
      :author "Paula Gearon"}
    naga.storage.memory.core
    (:require [clojure.set :as set]
              [clojure.core.cache :as c]
              [schema.core :as s]
              [naga.schema.structs :as st :refer [EPVPattern FilterPattern Pattern Results Value]]
              [naga.store :as store]
              [naga.util :as u]
              [naga.storage.memory.index :as mem])
    (:import [clojure.lang Symbol IPersistentVector IPersistentList]
             [naga.store Storage]))

(defprotocol Constraint
  (get-vars [c] "Returns a seq of the vars in a constraint")
  (left-join [c r g] "Left joins a constraint onto a result. Arguments in reverse order to dispatch on constraint type"))


(s/defn without :- [s/Any]
  "Returns a sequence minus a specific element"
  [e :- s/Any
   s :- [s/Any]]
  (remove (partial = e) s))


(s/defn paths :- [[EPVPattern]]
  "Returns a seq of all paths through the constraints. A path is defined
   by new patterns containing at least one variable common to the patterns
   that appeared before it. This prevents cross products in a join."
  ([patterns :- [EPVPattern]]
   (let [all-paths (paths #{} patterns)]
     (assert (every? (partial = (count patterns)) (map count all-paths))
             (str "No valid paths through: " (into [] patterns)))
     all-paths))
  ([bound :- #{Symbol}
    patterns :- [EPVPattern]]
   (apply concat
          (keep    ;; discard paths that can't proceed (they return nil)
           (fn [p]
             (let [b (get-vars p)]
               ;; only proceed when the pattern matches what has been bound
               (if (or (empty? bound) (seq (set/intersection b bound)))
                 ;; pattern can be added to the path, get the other patterns
                 (let [remaining (without p patterns)]
                   ;; if there are more patterns to add to the path, recurse
                   (if (seq remaining)
                     (map (partial cons p)
                          (seq
                           (paths (into bound b) remaining)))
                     [[p]])))))
           patterns))))


(def epv-pattern? vector?)
(def filter-pattern? list?)

(s/defn merge-filters
  "Merges filters into the sequence of patterns, so that they appear
   as soon as all their variables are first bound"
  [epv-patterns filter-patterns]
  (let [filter-vars (u/mapmap get-vars filter-patterns)
        all-bound-for? (fn [bound fltr] (every? bound (filter-vars fltr)))]
    (loop [plan [] bound #{} [np & rp :as patterns] epv-patterns filters filter-patterns]
      (if-not (seq patterns)
        ;; no patterns left, so apply remaining filters
        (concat plan filters)

        ;; divide the filters into those which are fully bound, and the rest
        (let [all-bound? (partial all-bound-for? bound)
              nxt-filters (filter all-bound? filters)
              remaining-filters (remove all-bound? filters)]
          ;; if filters were bound, append them, else get the next EPV pattern
          (if (seq nxt-filters)
            (recur (into plan nxt-filters) bound patterns remaining-filters)
            (recur (conj plan np) (into bound (get-vars np)) rp filters)))))))

(s/defn min-join-path :- [EPVPattern]
  "Calculates a plan based on no outer joins (a cross product), and minimized joins.
   A plan is the order in which to evaluate constraints and join them to the accumulated
   evaluated data. If it is not possible to create a path without a cross product,
   then return a plan of the patterns in the provided order."
  [patterns :- [Pattern]
   count-map :- {EPVPattern s/Num}]
  (or
   (->> (paths patterns)
        (sort-by (partial mapv count-map))
        first)
   patterns)) ;; TODO: longest paths with minimized cross products

(s/defn user-plan :- [EPVPattern]
  "Returns the original path specified by the user"
  [patterns :- [EPVPattern]
   _ :- {EPVPattern s/Num}]
  patterns)

(s/defn select-planner
  "Selects a query planner function"
  [options]
  (let [opt (into #{} options)]
    (case (get opt :planner)
      :user user-plan
      :min min-join-path
      min-join-path)))

(s/defn matching-vars :- {s/Num s/Num}
  "Returns pairs of indexes into seqs where the vars match.
   For any variable that appears in both sequences, the column number in the
   'from' parameter gets mapped to the column number of the same variable
   in the 'to' parameter."
  [from :- [s/Any]
   to :- [Symbol]]
  (->> to
       (keep-indexed
        (fn [nt vt]
          (seq
           (keep-indexed
            (fn [nf vf]
              (if (and (st/vartest? vf) (= vt vf))
                [nf nt]))
            from))))
       (apply concat)
       (into {})))

(s/defn modify-pattern :- [s/Any]
  "Creates a new EPVPattern from an existing one, based on existing bindings.
   Uses the mapping to copy from columns in 'existing' to overwrite variableis in 'pattern'.
   The variable locations have already been found and are in the 'mapping' argument"
  [existing :- [Value]
   mapping :- {s/Num s/Num}
   pattern :- EPVPattern]
  ;; TODO: this is in an inner loop. Is it faster to:
  ;;       (reduce (fn [p [f t]] (assoc p f t)) pattern mapping)
  (map-indexed (fn [n v]
                 (if-let [x (mapping n)]
                   (nth existing x)
                   v))
               pattern))

(s/defn pattern-left-join :- Results
  "Takes a partial result, and joins on the resolution of a pattern"
  [graph
   part :- Results
   pattern :- EPVPattern]
  (let [cols (:cols (meta part))
        total-cols (->> (st/vars pattern)
                        (remove (set cols))
                        (concat cols)
                        (into []))
        pattern->left (matching-vars pattern cols)]
    ;; iterate over part, lookup pattern
    (with-meta
      (for [lrow part
            :let [lookup (modify-pattern lrow pattern->left pattern)]
            rrow (mem/resolve-pattern graph lookup)]
        (concat lrow rrow))
      {:cols total-cols})))

(s/defn filter-join
  "Filters down results."
  [graph
   part :- Results
   fltr :- FilterPattern]
  (let [m (meta part)
        vars (:cols m)
        filter-fn (eval `(fn [~vars] ~fltr))]
    (with-meta (filter filter-fn part) m)))


;; protocol dispatch for patterns and filters in queries
(extend-protocol Constraint
  ;; EPVPatterns are implemented in vectors
  IPersistentVector
  (get-vars [p] (into #{} (st/vars p)))

  (left-join [p results graph] (pattern-left-join graph results p))

  ;; Filters are implemented in lists
  IPersistentList
  (get-vars [f] (:vars (meta f)))

  (left-join [f results graph] (filter-join graph results f)))


(s/defn plan-path :- [(s/one [Pattern] "Patterns in planned order")
                      (s/one {EPVPattern Results} "Single patterns mapped to their resolutions")]
  "Determines the order in which to perform the elements that go into a query.
   Tries to optimize, so it uses the graph to determine some of the
   properties of the query elements. Options can describe which planner to use.
   Planning will determine the resolution map, and this is returned with the plan.
   By default the min-join-path function is used. This can be overriden with options:
     [:planner plan]
   The plan can be one of :user, :min.
   :min is the default. :user means to execute in provided order."
  [graph
   patterns :- [Pattern]
   options]
  (let [epv-patterns (filter epv-pattern? patterns)
        filter-patterns (filter filter-pattern? patterns)

        resolution-map (u/mapmap (partial mem/resolve-pattern graph)
                                 epv-patterns)

        count-map (u/mapmap (comp count resolution-map) epv-patterns)

        query-planner (select-planner options)

        ;; run the query planner
        planned (query-planner epv-patterns count-map)
        plan (merge-filters planned filter-patterns)]

    ;; result
    [plan resolution-map]))


(s/defn join-patterns :- Results
  "Joins the resolutions for a series of patterns into a single result."
  [graph
   patterns :- [Pattern]
   & options]
  (let [[[fpath & rpath] resolution-map] (plan-path graph patterns options)
        ;; execute the plan by joining left-to-right
        ;; left-join has back-to-front params for dispatch reasons
        ljoin #(left-join %2 %1 graph)
        part-result (with-meta
                      (resolution-map fpath)
                      {:cols (st/vars fpath)})]
    (reduce ljoin part-result rpath)))


(s/defn project :- Results
  "Converts each row from a result, into just the requested columns, as per the pattern arg.
   Any specified value in the pattern will be copied into that position in the projection.
  e.g. For pattern [?h1 :friend ?h2]
       data: [[h1=frodo h3=bilbo h2=gandalf]
              [h1=merry h3=pippin h2=frodo]]
  leads to: [[h1=frodo :friend h2=gandalf]
             [h1=merry :friend h2=frodo]]"
  [pattern :- [s/Any]
   data :- Results]
  (let [pattern->data (matching-vars pattern (:cols (meta data)))]
    (map #(modify-pattern % pattern->data pattern) data)))

(s/defn add-to-graph
  [graph
   data :- Results]
  (reduce (fn [acc d] (apply mem/graph-add acc d)) graph data))

;; Using a cache of 1 is currently redundant to an atom
(let [m (atom (c/lru-cache-factory {} :threshold 1))]
  (defn get-count-fn
    "Returns a memoized counting function for the current graph.
     These functions only last as long as the current graph."
    [graph]
    (if-let [f (c/lookup @m graph)]
      (do
        (swap! m c/hit graph)
        f)
      (let [f (memoize #(count (mem/resolve-pattern graph %)))]
        (swap! m c/miss graph f)
        f))))

(defrecord MemoryStore [graph]
  Storage
  (start-tx [this] this)

  (commit-tx [this] this)
  
  (resolve-pattern [_ pattern]
    (mem/resolve-pattern graph pattern))

  (count-pattern [_ pattern]
    (if-let [count-fn (get-count-fn graph)]
      (count-fn pattern)
      (count (mem/resolve-pattern graph pattern))))

  (query [_ output-pattern patterns]
    (->> (join-patterns graph patterns)
         (project output-pattern)))

  (assert-data [_ data]
    (->MemoryStore (add-to-graph graph data)))

  (query-insert [this assertion-pattern patterns]
    (->> (join-patterns graph patterns)
         (project assertion-pattern)
         (add-to-graph graph)
         ->MemoryStore)))

(def empty-store (->MemoryStore mem/empty-graph))

(s/defn create-store :- Storage
  "Factory function to create a store"
  [config]
  empty-store)

(store/register-storage! :memory create-store)
