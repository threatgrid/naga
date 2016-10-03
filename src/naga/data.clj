(ns ^{:doc "Converts external data into a graph format (triples)."
      :author "Paula Gearon"}
    naga.data
  (:require [schema.core :as s :refer [=>]]
            [clojure.java.io :as io]
            [cheshire.core :as j]
            [naga.store :as store])
  (:import [java.util Map List]))

(def ^:dynamic *current-storage* nil)

(defmulti value-triples
  "Converts a value into a list of triples.
   Return the entity ID of the data coupled with the sequence of triples.
   NOTE: This may need to be dispatched to storage.
         e.g. Datomic could use properties to determine how to encode data."
  type)

(declare map->triples)

(defmethod value-triples List
  [[v & vs :as vlist]]
  (if (seq vlist)
    (let [id (store/new-node *current-storage*)
          [value-id triples] (value-triples v)
          [next-id next-triples] (value-triples vs)]
      [id (concat [[id :naga/first value-id]
                   [id :naga/rest next-id]]
                  triples
                  next-triples)])
    [:naga/nil nil]))

(defmethod value-triples Map      [v] (map->triples v))

(defmethod value-triples nil      [v] [:naga/nil nil])

(defmethod value-triples :default [v] [v nil])


(s/defn property-vals :- [[s/Any s/Keyword s/Any]]
  "Takes a property-value pair associated with an entity,
   and builds triples around it"
  [entity-id :- s/Any
   [property value] :- [s/Keyword s/Any]]
  (let [[value-id value-data] (value-triples value)]
    (cons [entity-id property value-id] value-data)))


(s/defn map->triples :- [s/Any [[s/Any s/Keyword s/Any]]]
  "Converts a single map to triples"
  [data :- {s/Any s/Any}]
  (let [entity-id (or (:db/id data) (store/new-node *current-storage*))
        triples-data (mapcat (partial property-vals entity-id)
                             data)]
    [entity-id triples-data]))


(s/defn json->triples
  "Converts parsed JSON into a sequence of triples for a provided storage."
  [storage j]
  (binding [*current-storage* storage]
    (doall (mapcat second
                   (map map->triples j)))))


(s/defn stream->triples :- [[s/Any s/Keyword s/Any]]
  "Converts a stream to triples relevant to a store"
  [storage io]
  (with-open [r (io/reader io)]
    (let [data (j/parse-stream r true)]
      (json->triples storage data))))


(s/defn string->triples :- [[s/Any s/Keyword s/Any]]
  "Converts a string to triples relevant to a store"
  [storage s]
  (json->triples storage (j/parse-string s true)))
