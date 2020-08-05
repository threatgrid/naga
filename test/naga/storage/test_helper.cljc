(ns naga.storage.test-helper
  (:require [naga.store :as store :refer [Storage]]
            [zuko.node :refer [NodeAPI]]
            #?(:clj  [schema.core :as s]
               :cljs [schema.core :as s :include-macros true])))

(declare ->TestStore)

(s/defrecord TestStore [data n]
  Storage
  (start-tx [store] store)
  (commit-tx [store] store)
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
      store))
  NodeAPI
  (new-node [store]
    (let [v (swap! n inc)]
      (keyword "test" (str "n" v))))
  (node-type? [store p n] (and (keyword? n)
                               (= "test" (namespace n))
                               (= \n (first (name n)))))
  (data-attribute [store data] :naga/first)
  (container-attribute [store data] :naga/contains)
  (find-triple [store pattern] data))

(defn new-store [] (->TestStore [0] (atom 0)))

(def empty-store (new-store))
