(ns naga.storage.memory
  (:require [clojure.set :as set]
            [schema.core :as s]
            [naga.structs :as st :refer [EPVPattern Results]]
            [naga.store :as store]
            [naga.util :as u]
            [naga.storage.memory-index :as mem]
            )
  (:import [clojure.lang Symbol]
           [naga.store Storage]))

(s/defn without :- [s/Any]
  "Returns a sequence minus a specific element"
  [e :- s/Any
   s :- [s/Any]]
  (remove (partial = e) s))

(s/defn vartest? :- s/Bool
  [x]
  (and (symbol? x) (= \? (first (name x)))))

(s/defn bindings :- #{Symbol}
  "Return a seq of all variables in a pattern"
  [pattern :- EPVPattern]
  (filter vartest? pattern))

(s/defn bindings-set :- #{Symbol}
  "Return a set of all variables in a pattern"
  [pattern :- EPVPattern]
  (into #{} (bindings pattern)))

(s/defn paths :- [[EPVPattern]]
  "Returns a seq of all paths through the constraints"
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
             (let [b (bindings-set p)]
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
  "Calculates a plan based on no outer joins, and minimized joins"
  [patterns :- [EPVPattern]
   count-map :- {EPVPattern s/Num}]
  (->> (paths patterns)
       (sort-by (partial mapv count-map))
       first))

(s/defn user-plan
  "Returns the original path specified by the user"
  [patterns :- [EPVPattern]
   _ :- {EPVPattern s/Num}]
  patterns)

(s/defn select-planner
  "Selects a query planner"
  [options]
  (let [opt (into #{} options)]
    (condp #(get %2 %1) opt
      :user user-plan
      :min min-join-path
      min-join-path)))

(s/defn matching-vars :- {s/Num s/Num}
  "Returns pairs of indexes into seqs where the vars match"
  [from :- [Symbol]
   to :- [s/Any]]
  (->> to
       (keep-indexed
        (fn [nt vt]
          (seq
           (keep-indexed
            (fn [nf vf]
              (if (and (vartest? vf) (= vt vf))
                [nf nt]))
            from))))
       (apply concat)
       (into {})))

(s/defn modify-pattern :- EPVPattern
  "Creates a new EPVPattern from an existing one, based on existing bindings."
  [existing :- [Symbol]
   mapping :- {s/Num s/Num}
   pattern :- EPVPattern]
  (map-indexed (fn [n v]
                 (if-let [x (mapping n)]
                   (nth existing x)
                   v))
               pattern))

(s/defn left-join :- Results
  "Takes a partial result, and joins on the resolution of a pattern"
  [graph :- mem/Graph
   part :- Results
   pattern :- EPVPattern]
  (let [cols (:cols (meta part))
        new-cols (into [] (concat cols (bindings pattern)))
        pattern->left (matching-vars pattern cols)]
    ;; iterate over part, lookup pattern
    (with-meta
      (for [lrow part
            :let [lookup (modify-pattern lrow pattern->left pattern)]
            rrow (mem/resolve-pattern graph lookup)]
        (concat lrow rrow))
      {:cols new-cols})))

(s/defn join-patterns :- Results
  "Joins the resolutions for a series of patterns into a single result."
  [graph :- mem/Graph
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
                      {:cols fpath})]

    (reduce ljoin part-result rpath)))


(s/defn project :- Results
  [pattern :- [s/Any]
   data :- Results]
  (let [pattern->data (matching-vars pattern data)]
    (map #(modify-pattern % pattern->data pattern) data)))

(s/defn add-to-graph
  [graph :- mem/Graph
   data :- Results]
  (reduce (partial apply mem/graph-add) graph data))

(defrecord MemoryStore [graph]
  Storage
  (resolve [_ pattern]
    (mem/resolve-pattern graph pattern))

  (join [_ output-pattern patterns]
    (->> (join-patterns graph patterns)
         (project output-pattern)))

  (assert-data [_ data]
    (add-to-graph graph data))

  (query-insert [this assertion-pattern patterns]
    (add-to-graph graph (join-patterns this assertion-pattern patterns))))
