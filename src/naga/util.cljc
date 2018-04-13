(ns naga.util
    "The ubiquitous utility namespace that every project seems to have"
    (:require [schema.core :as s :refer [=>]]
               #?(:cljs [cljs.js :as js :refer [eval empty-state js-eval]]))
    #?(:clj (:import [clojure.lang Var])))

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

#?(:clj
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

   :cljs
   (s/defn get-fn-reference :- (s/maybe Var)
     "Looks up a namespace:name function represented in a keyword,
      and if it exists, return it. Otherwise nil"
     [kw :- (s/cond-pre s/Keyword s/Symbol)]
     (when-let [nms (namespace kw)]
       (let [snm (symbol nms (name kw))]
         (try
           (:value
            (cljs.js/eval (cljs.js/empty-state)
                          snm
                          {:eval cljs.js/js-eval :source-map true :context :expr}
                          identity))
           (catch :default _ ))))))

(s/defn divide' :- [[s/Any] [s/Any]]
  "Takes a predicate and a sequence and returns 2 sequences.
   The first is where the predicate returns true, and the second
   is where the predicate returns false. Note that a nil value
   will not be returned in either sequence, regardless of the
   value returned by the predicate."
  [p
   s :- [s/Any]]
  (let [d (map (fn [x] (if (p x) [x nil] [nil x])) s)]
    [(keep first d) (keep second d)]))

(defn fixpoint
  "Applies the function f to the value a. The function is then,
   and applied to the result, over and over, until the result does not change.
   Returns the final result.
   Note: If the function has no fixpoint, then runs forever."
  [f a]
  (let [s (iterate f a)]
    (some identity (map #(#{%1} %2) s (rest s)))))
