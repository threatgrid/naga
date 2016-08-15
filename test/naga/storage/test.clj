(ns naga.storage.test
  (:require [naga.store :as store :refer [Storage]]
            [schema.core :as s]))

(s/defrecord TestStore [data]
  Storage
  (resolve [store pattern] data)
  (join [store output-pattern patterns] data)
  (assert-data [store new-data]
    (if (> 4 (count data))
      (->TestStore (conj data 0))
      store))
  (query-insert [store assertion-pattern patterns]
    (if (> 4 (count data))
      (->TestStore (conj data 0))
      store)))

(def empty-store (->TestStore [0]))
