(ns ^{:doc "A storage implementation over in-memory indexing. Includes full query engine."
      :author "Paula Gearon"}
    naga.storage.memory.core
    (:require [clojure.set :as set]
              [schema.core :as s]
              [naga.schema.structs :as st :refer [EPVPattern Results Value]]
              [naga.store :as store]
              [naga.util :as u]
              [naga.storage.memory.index :as mem])
    (:import [clojure.lang Symbol]
             [naga.store Storage]))

(s/defn without :- [s/Any]
  "Returns a sequence minus a specific element"
  [e :- s/Any
   s :- [s/Any]]
  (remove (partial = e) s))

(s/defn vars :- [Symbol]
  "Return a seq of all variables in a pattern"
  [pattern :- EPVPattern]
  (filter mem/vartest? pattern))

(s/defn vars-set :- #{Symbol}
  "Return a set of all variables in a pattern"
  [pattern :- EPVPattern]
  (into #{} (vars pattern)))

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
             (let [b (vars-set p)]
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

(s/defn min-join-path
  "Calculates a plan based on no outer joins (a cross product), and minimized joins.
   A plan is the order in which to evaluate constraints and join them to the accumulated
   evaluated data. If it is not possible to create a path without a cross product,
   then return a plan of the patterns in the provided order."
  [patterns :- [EPVPattern]
   count-map :- {EPVPattern s/Num}]
  (or
   (->> (paths patterns)
        (sort-by (partial mapv count-map))
        first)
   patterns)) ;; TODO: longest paths with minimized cross products

(s/defn user-plan
  "Returns the original path specified by the user"
  [patterns :- [EPVPattern]
   _ :- {EPVPattern s/Num}]
  patterns)

(s/defn select-planner
  "Selects a query planner function"
  [options]
  (let [opt (into #{} options)]
    (condp #(get %2 %1) opt
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
              (if (and (mem/vartest? vf) (= vt vf))
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

(s/defn left-join :- Results
  "Takes a partial result, and joins on the resolution of a pattern"
  [graph
   part :- Results
   pattern :- EPVPattern]
  (let [cols (:cols (meta part))
        total-cols (->> (vars pattern)
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

(s/defn join-patterns :- Results
  "Joins the resolutions for a series of patterns into a single result."
  [graph
   patterns :- [EPVPattern]
   & options]
  (let [resolution-map (u/mapmap (fn [p]
                                   (if-let [{r :resolution} (meta p)]
                                     r
                                     (mem/resolve-pattern graph p)))
                                 patterns)

        count-map (u/mapmap (comp count resolution-map) patterns)

        query-planner (select-planner options)

        ;; run the query planner
        [fpath & rpath] (query-planner patterns count-map)

        ;; execute the plan by joining left-to-right
        ljoin (partial left-join graph)

        part-result (with-meta
                      (resolution-map fpath)
                      {:cols (vars fpath)})]

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

(defrecord MemoryStore [graph]
  Storage
  (start-tx [this] this)

  (commit-tx [this] this)
  
  (resolve-pattern [_ pattern]
    (mem/resolve-pattern graph pattern))

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
