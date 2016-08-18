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

(s/defn bindings :- #{Symbol}
  "Return a set of all variables in a pattern"
  [pattern :- EPVPattern]
  (->> pattern
       (filter #(and (symbol? %) (= \? (first (name %)))))
       (into #{})))

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
             (let [b (bindings p)]
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

(s/defn left-join :- [[s/Any]]
  "Takes a partial result, and joins on the resolution of a pattern"
  [store :- Storage
   part :- [[s/Any]]
   pattern :- EPVPattern]
  (let [cols (:cols (meta part))
        new-cols (concat cols pattern)]
    ;; identify lookup columns by intersecting names
    ;; map to index type by lookup col positions
    ;; iterate over part, lookup pattern
    ;; wrap with (with-meta result {:cols new-cols})
    ))

(s/defn join :- [[s/Any]]
  "Joins the resolutions for a series of patterns into a single result."
  [store :- Storage
   patterns :- [EPVPattern]]
  (let [resolution-map (u/mapmap (fn [p]
                                   (if-let [{r :resolution} (meta p)]
                                     r
                                     (store/resolve store p)))
                                 patterns)

        count-map (u/mapmap (comp count resolution-map) patterns)

        ;; run the query optimizer
        [fpath & rpath] (->> (paths patterns)
                             (sort-by (partial mapv count-map))
                             first)

        ljoin (partial left-join store)

        part-result (with-meta
                      (resolution-map fpath)
                      {:cols fpath})]

    (reduce ljoin part-result (rest path))))
