(ns naga.storage.test
  (:require [naga.store :as store :refer [Storage]]
            #?(:clj  [schema.core :as s]
               :cljs [schema.core :as s :include-macros true])))

(declare ->TestStore)

(s/defrecord TestStore [data n]
  Storage
  (start-tx [store] store)
  (commit-tx [store] store)
  (new-node [store]
    (let [v (swap! n inc)]
      (keyword "test" (str "n" v))))
  (node-type? [store p n] (and (keyword? n)
                               (= "test" (namespace n))
                               (= \n (first (name n)))))
  (data-property [store data] :naga/first)
  (container-property [store data] :naga/contains)
  (count-pattern [store pattern] (count data))
  (resolve-pattern [store pattern] data)
  (query [store output-pattern patterns] data)
  (assert-data [store new-data]
    (if (> 4 (count data))
      (->TestStore (conj data 0) (atom 0))
      store))
  (query-insert [store assertion-pattern patterns]
    (if (> 4 (count data))
      (->TestStore (conj data 0) (atom 0))
      store)))

(defn new-store [] (->TestStore [0] (atom 0)))

(def empty-store (new-store))
