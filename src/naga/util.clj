(ns ^{:doc "The ubiquitous utility namespace that every project seems to have"
      :author "Paula Gearon"}
    naga.util
  (:require [schema.core :as s :refer [=>]])
  (:import [clojure.lang Var]))

(s/defn mapmap :- {s/Any s/Any}
  "Creates a map from functions applied to a seq.
   (map (partial * 2) [1 2 3 4 5])
     => {1 2, 2 4, 3 6, 4 8, 5 10}
   (map #(keyword (str \"k\" (dec %))) (partial * 3) [1 2 3])
     => {:k0 3, :k1 6, :k2 9}"
  ([valfn :- (=> s/Any s/Any)
    s :- [s/Any]] (mapmap identity valfn s))
  ([keyfn :- (=> s/Any s/Any)
    valfn :- (=> s/Any s/Any)
    s :- [s/Any]]
    (into {} (map (juxt keyfn valfn) s))))

(s/defn get-fn-reference :- (s/maybe Var)
  "Looks up a namespace:name function represented in a keyword,
   and if it exists, return it. Otherwise nil"
  [kw :- (s/cond-pre s/Keyword s/Symbol)]
  (let [kns (namespace kw)
        snm (symbol (name kw))]
    (some-> kns
      symbol
      find-ns
      (ns-resolve snm))))
