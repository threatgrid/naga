(ns ^{:doc "Converts external data into a graph format (triples)."
      :author "Paula Gearon"}
    naga.data
  (:require [schema.core :as s :refer [=>]]
            [clojure.java.io :as io]
            [cheshire.core :as j]
            [naga.store :as store :refer [Storage]])
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
  [storage :- Storage
   s :- s/Str]
  (json->triples storage (j/parse-string s true)))


;; extracting from the store


(s/defn property-values :- [[s/Keyword s/Any]]
  "Return all the property/value pairs for a given entity in the store."
  [store :- Storage
   entity :- s/Any]
  (store/resolve-pattern store [entity '?p '?o]))


(s/defn check-structure :- (s/maybe [[s/Keyword s/Any]])
  "Determines if a value represents a structure. If so, return the property/values for it.
   Otherwise, return nil."
  [store :- Storage
   prop :- s/Any
   v :- s/Any]
  (if (store/node-type? store prop v)
    (let [data (property-values store v)]
      (seq data))))


(declare pairs->json)


(s/defn build-list
  "Takes property/value pairs and if they represent a list node, returns the list.
   else, nil."
  [store :- Storage
   [[fprop fval] [sprop sval] :as pairs] :- [[s/Keyword s/Any]]]
  (letfn [(build [untested-prop elt rem-list]
             (assert (= :naga/rest untested-prop))
             (cons elt (build-list store (property-values store rem-list))))]
    (if (= :naga/first fprop)
      (build sprop fval sval)
      (build fprop sval fval))))


(s/defn recurse-node :- s/Any
  "Determines if the val of a map entry is a node to be recursed on, and loads if necessary"
  [store :- Storage
   [prop v] :- [s/Keyword s/Any]]
  (if-let [pairs (check-structure store prop v)]
    (or (build-list store pairs)
        (pairs->json store pairs))
    v))


(s/defn id->json :- {s/Keyword s/Any}
  "Uses a set of property-value pairs to load up a nested data structure from the graph"
  [store :- Storage
   prop-vals :- [s/Any s/Any]]
  (->> prop-vals
       (map (partial recurse-node store))
       (into {})))


(s/defn pairs->json :- {s/Keyword s/Any}
  "Uses an id node to load up a nested data structure from the graph"
  [store :- Storage
   entity :- s/Any]
  (id->json store entity (property-values store entity)))


(s/defn ident->json :- {s/Keyword s/Any}
  "Converts data in a database to data structures suitable for JSON encoding"
  [store :- Storage
   ident :- s/Any]
  ;; find the entity by its ident. Some systems will make the id the entity id,
  ;; and the ident will be separate, so look for both.
  (let [eid (or (ffirst (store/resolve-pattern store '[?eid :db/id ident]))
                (ffirst (store/resolve-pattern store '[?eid :db/ident ident])))]
    (id->json store eid)))
