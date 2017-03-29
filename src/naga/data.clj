(ns ^{:doc "Converts external data into a graph format (triples)."
      :author "Paula Gearon"}
    naga.data
  (:require [schema.core :as s :refer [=>]]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [cheshire.core :as j]
            [naga.store :as store :refer [Storage]])
  (:import [java.util Map List]))

(def ^:dynamic *current-storage* nil)

(def Triple [s/Any s/Keyword s/Any])

(defn get-naga-first
  "Finds the naga/first property in a map, and gets the value."
  [struct]
  (let [first-pair? (fn [[k v :as p]]
                     (and (= "naga" (namespace k))
                          (str/starts-with? (name k) "first")
                          p))]
    (some first-pair? struct)))

(s/defn containership-triples
  "Finds the list of entity nodes referred to in a list and builds
   triples describing a flat 'contains' property"
  [node :- s/Any
   triples :- [Triple]]
  (let [listmap (->> (group-by first triples)
                     (map (fn [[k vs]]
                            [k (into {} (map #(apply vector (rest %)) vs))]))
                     (into {}))
        node-list (loop [nl [] n node]
                    (if-not n
                      nl
                      (let [{r :naga/rest :as lm} (listmap n)
                            [_ f] (get-naga-first lm)]
                        (recur (conj nl f) r))))]
    (map
     (fn [n] [node (store/container-property *current-storage* n) n])
     node-list)))

(defmulti value-triples
  "Converts a value into a list of triples.
   Return the entity ID of the data coupled with the sequence of triples.
   NOTE: This may need to be dispatched to storage.
         e.g. Datomic could use properties to determine how to encode data."
  type)

(declare map->triples)

(s/defn list-triples
  "Creates the triples for a list"
  [[v & vs :as vlist]]
  (if (seq vlist)
    (let [id (store/new-node *current-storage*)
          [value-id triples] (value-triples v)
          [next-id next-triples] (list-triples vs)]
      [id (concat [[id (store/data-property *current-storage* value-id) value-id]]
                  (when next-id [[id :naga/rest next-id]])
                  triples
                  next-triples)])))

(defmethod value-triples List
  [vlist]
  (let [[node triples :as raw-result] (list-triples vlist)]
    (if triples
      [node (concat triples (containership-triples node triples))]
      raw-result)))

(defmethod value-triples Map      [v] (map->triples v))

(defmethod value-triples nil      [v] nil)

(defmethod value-triples :default [v] [v nil])


(s/defn property-vals :- [Triple]
  "Takes a property-value pair associated with an entity,
   and builds triples around it"
  [entity-id :- s/Any
   [property value] :- [s/Keyword s/Any]]
  (if-let [[value-id value-data] (value-triples value)]
    (cons [entity-id property value-id] value-data)))


(s/defn map->triples :- [s/Any [Triple]]
  "Converts a single map to triples. Returns a pair of the map's ID and the triples for the map."
  [data :- {s/Keyword s/Any}]
  (let [entity-id (or (:db/id data) (store/new-node *current-storage*))
        triples-data (mapcat (partial property-vals entity-id)
                             data)]
    [entity-id triples-data]))


(s/defn name-for
  "Convert an id (probably a number) to a keyword for identification"
  [id :- s/Any]
  (if (keyword? id)
    id
    (store/node-label *current-storage* id)))


(s/defn ident-map->triples :- [s/Any [Triple]]
  "Converts a single map to triples for an ID'ed map"
  [j]
  (let [[id triples] (map->triples j)]
    (if (:db/ident j)
      triples
      (cons [id :db/ident (name-for id)] triples))))


(s/defn json->triples :- [Triple]
  "Converts parsed JSON into a sequence of triples for a provided storage."
  [storage j]
  (binding [*current-storage* storage]
    (doall (mapcat ident-map->triples j))))


(s/defn stream->triples :- [Triple]
  "Converts a stream to triples relevant to a store"
  [storage io]
  (with-open [r (io/reader io)]
    (let [data (j/parse-stream r true)]
      (json->triples storage data))))


(s/defn string->triples :- [Triple]
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
  (if (and (not (#{:db/ident :db/id} prop)) (store/node-type? store prop v))
    (let [data (property-values store v)]
      (seq data))))


(declare pairs->json recurse-node)

(s/defn build-list :- [s/Any]
  "Takes property/value pairs and if they represent a list node, returns the list.
   else, nil."
  [store :- Storage
   pairs :- [[s/Keyword s/Any]]]
  ;; convert the data to a map
  (let [st (into {} pairs)]
    ;; if the properties indicate a list, then process it
    (when-let [first-prop-elt (get-naga-first st)]
      (let [remaining (:naga/rest st)
            [_ first-elt] (recurse-node store first-prop-elt)]
        (assert first-elt)
        ;; recursively build the list
        (cons first-elt (build-list store (property-values store remaining)))))))


(s/defn recurse-node :- s/Any
  "Determines if the val of a map entry is a node to be recursed on, and loads if necessary"
  [store :- Storage
   [prop v :as prop-val] :- [s/Keyword s/Any]]
  (if-let [pairs (check-structure store prop v)]
    [prop (or (build-list store pairs)
              (pairs->json store pairs))]
    prop-val))


(s/defn pairs->json :- {s/Keyword s/Any}
  "Uses a set of property-value pairs to load up a nested data structure from the graph"
  [store :- Storage
   prop-vals :- [[s/Keyword s/Any]]]
  (dissoc
   (->> prop-vals
        (map (partial recurse-node store))
        (into {}))
   :db/id
   :db/ident))


(s/defn id->json :- {s/Keyword s/Any}
  "Uses an id node to load up a nested data structure from the graph"
  [store :- Storage
   entity-id :- s/Any]
  (pairs->json store (property-values store entity-id)))


(s/defn ident->json :- {s/Keyword s/Any}
  "Converts data in a database to data structures suitable for JSON encoding"
  [store :- Storage
   ident :- s/Any]
  ;; find the entity by its ident. Some systems will make the id the entity id,
  ;; and the ident will be separate, so look for both.
  (let [eid (or (ffirst (store/resolve-pattern store '[?eid :db/id ident]))
                (ffirst (store/resolve-pattern store '[?eid :db/ident ident])))]
    (id->json store eid)))

(s/defn store->json :- [{s/Keyword s/Any}]
  "Pulls all top level JSON out of a store"
  [store :- Storage]
  (->> (store/resolve-pattern store '[?e :db/ident ?id])
       (map first)
       (map (partial id->json store))))

(s/defn store->str :- s/Str
  "Reads a store into JSON strings"
  [store :- Storage]
  (j/generate-string (store->json store)))
