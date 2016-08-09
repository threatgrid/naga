(ns naga.engine
  (:require [naga.structs :as st :refer [EPVPattern RulePAtternPair Body]]
            [naga.queue :as q]
            [naga.store :as store]
            [schema.core :as s])
  (:import [naga.structs Rule]))

(def ^:private new-state {:symbol-map {} :cntr 0})

(defn- skolemize-symbol
  "Used by skolemize to find or create a new symbol to replace any var symbols.
   element - an element that may contain a symbol. If so, it will be replaced.
   state - keeps track of symbols replacements.
   result: the element, or its replacement, and the new state after replacement."
  [element {:keys [symbol-map cntr] :as state}]
  (if (and (symbol? element) (= \? (first (name element))))
    (if-let [mapped-symbol (symbol-map element)]
      [mapped-symbol state]
      (let [mapped-symbol (symbol (str "?v" cntr))]
        [mapped-symbol {:symbol-map (assoc symbol-map element mapped-symbol)
                        :cntr (inc cntr)}]))
    [element state]))
  
(s/defn skolemize* :- EPVPattern
  "Skolemize a pattern to use consistent naming"
  [pattern :- EPVPattern]
  (->> pattern
       (reduce (fn [[result-pattern state] elt]
                 (let [[skol-sym new-state] (skolemize-symbol elt state)]
                   [(conj result-pattern skol-sym) new-state]))
               [[] new-state])
       first))

(def skolemize (memoize skolemize*))


(s/defn run :- s/Bool
  "Runs a program against a given configuration"
  [config :- {s/Keyword s/Any}
   program :- Program]
  (let [storage (store/get-storage-handle config)
        axioms (filter vector? program)
        rules (remove vector? program)]
    (store/assert-data storage-handle axioms)
    (execute rules storage-handle)))

(s/defn execute
  "Executes a program"
  [rules :- [Rule]
   storage :- {s/Keyword s/Any}]
  (let [rule-queue (reduce q/add
                           (q/new-queue :salience :name)
                           rules)]
    ))
