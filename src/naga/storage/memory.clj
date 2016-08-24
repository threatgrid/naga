(ns naga.storage.memory
  (:require [clojure.set :as set]
            [schema.core :as s]
            [naga.structs :as st :refer [EPVPattern]]
            [naga.store :as store]
            [naga.util :as u])
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

(s/defn resolve
  [{:graph store} [s p o g]]
  (memory/resolve-pattern graph pattern))

(s/defn keep-indexed :- [s/Any]
  [f :- (=> s/Any [s/Num s/Any])
   coll :- [s/Any]]
  (->> (map-indexed f coll)
       (remove nil?)))

(s/defn left-join :- [[s/Any]]
  "Takes a partial result, and joins on the resolution of a pattern"
  [store :- Storage
   part :- [[s/Any]]
   pattern :- EPVPattern]
  (let [cols (:cols (meta part))
        pattern-cols (bindings pattern)
        new-cols (into [] (concat cols pattern-cols))
        ;; identify lookup columns by intersecting names
        pattern->left (apply concat
                             (keep-indexed
                              (fn [nr vr]
                                (if (vartest? vr)
                                  (keep-indexed
                                   (fn [nl vl] (if (= vl vr) [nr nl]))
                                   cols)))
                              pattern))]
    ;; iterate over part, lookup pattern
    (with-meta
      (for [lrow part
            :let [lookup (map-indexed (fn [n v]
                                        (if-let [x (pattern->left n)]
                                          (nth lrow x)
                                          v))
                                      pattern)]
            rrow (resolve-pattern store lookup)]
        (concat lrow rrow))
      {:cols new-cols})))

(s/defn join :- [[s/Any] & options]
  "Joins the resolutions for a series of patterns into a single result."
  [store :- Storage
   patterns :- [EPVPattern]]
  (let [
        resolution-map (u/mapmap (fn [p]
                                   (if-let [{r :resolution} (meta p)]
                                     r
                                     (resolve-pattern store p)))
                                 patterns)

        count-map (u/mapmap (comp count resolution-map) patterns)

        query-planner (select-planner opt)

        ;; run the query planner
        [fpath & rpath] (query-planner patterns count-map)

        ;; execute the plan by joining left-to-right
        ljoin (partial left-join store)

        part-result (with-meta
                      (resolution-map fpath)
                      {:cols fpath})]

    (reduce ljoin part-result (rest path))))
