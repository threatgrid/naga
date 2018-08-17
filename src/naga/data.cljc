(ns ^{:doc "Converts external data into a graph format (triples)."
      :author "Paula Gearon"}
    naga.data
  (:require [schema.core :as s :refer [=>]]
            [clojure.string :as str]
            #?(:clj [clojure.java.io :as io])
            #?(:clj [cheshire.core :as j])
            [naga.store :as store :refer [StorageType]])
  #?(:clj (:import [java.util Map List])))

#?(:clj  (def parse-json-string #(j/parse-string % true))
   :cljs (def parse-json-string #(js->clj (.parse js/JSON %) :keywordize-keys true)))

#?(:clj
   (defn json-generate-string
     ([data] (j/generate-string data))
     ([data indent]
      (j/generate-string
       data
       (assoc j/default-pretty-print-options
              :indentation (apply str (repeat indent \space))))))

   :cljs
   (defn json-generate-string
     ([data] (.stringify js/JSON (clj->js data)))
     ([data indent] (.stringify js/JSON (clj->js data) nil indent))))

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
    (doall  ;; uses a dynamically bound value, so ensure that this is executed
      (map
       (fn [n] [node (store/container-property *current-storage* n) n])
       node-list))))

(declare value-triples map->triples)

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

(s/defn value-triples-list :- [(s/one s/Any "node ID") (s/one [Triple] "current list of triples")]
  [vlist :- [s/Any]]
  (let [[node triples :as raw-result] (list-triples vlist)]
    (if triples
      [node (concat triples (containership-triples node triples))]
      raw-result)))

(s/defn value-triples
  "Converts a value into a list of triples.
   Return the entity ID of the data coupled with the sequence of triples.
   NOTE: This may need to be dispatched to storage.
         e.g. Datomic could use properties to determine how to encode data."
  [v]
  (cond
    (sequential? v) (value-triples-list v)
    (set? v) (value-triples-list v)
    (map? v) (map->triples v)
    (nil? v) nil
    :default [v nil]))

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
        triples-data (doall (mapcat (partial property-vals entity-id)
                                    data))]
    [entity-id triples-data]))


(s/defn name-for
  "Convert an id (probably a number) to a keyword for identification"
  [id :- s/Any]
  (if (keyword? id)
    id
    (store/node-label *current-storage* id)))


(s/defn ident-map->triples :- [s/Any [Triple]]
  "Converts a single map to triples for an ID'ed map"
  ([storage j]
   (binding [*current-storage* storage]
     (ident-map->triples j)))
  ([j]
   (let [[id triples] (map->triples j)]
     (if (:db/ident j)
       triples
       (concat [[id :db/ident (name-for id)] [id :naga/entity true]] triples)))))


(s/defn json->triples :- [Triple]
  "Converts parsed JSON into a sequence of triples for a provided storage."
  [storage j]
  (binding [*current-storage* storage]
    (doall (mapcat ident-map->triples j))))


#?(:clj
    (s/defn stream->triples :- [Triple]
      "Converts a stream to triples relevant to a store"
      [storage io]
      (with-open [r (io/reader io)]
        (let [data (j/parse-stream r true)]
          (json->triples storage data))))
    
   :cljs
    (s/defn stream->triples :- [Triple]
      [storage io]
      (throw (ex-info "Unsupported IO" {:io io}))))

(s/defn string->triples :- [Triple]
  "Converts a string to triples relevant to a store"
  [storage :- StorageType
   s :- s/Str]
  (json->triples storage (parse-json-string s)))


;; extracting from the store


(s/defn property-values :- [[s/Keyword s/Any]]
  "Return all the property/value pairs for a given entity in the store.
   Skips non-keyword properties, as these are not created by naga.data"
  [store :- StorageType
   entity :- s/Any]
  (->> (store/resolve-pattern store [entity '?p '?o])
       (filter (comp keyword? first))))


(s/defn check-structure :- (s/maybe [[s/Keyword s/Any]])
  "Determines if a value represents a structure. If so, return the property/values for it.
   Otherwise, return nil."
  [store :- StorageType
   prop :- s/Any
   v :- s/Any]
  (if (and (not (#{:db/ident :db/id} prop)) (store/node-type? store prop v))
    (let [data (property-values store v)]
      data)))


(declare pairs->json recurse-node)

(s/defn build-list :- [s/Any]
  "Takes property/value pairs and if they represent a list node, returns the list.
   else, nil."
  [store :- StorageType
   pairs :- [[s/Keyword s/Any]]]
  ;; convert the data to a map
  (let [st (into {} pairs)]
    ;; if the properties indicate a list, then process it
    (when-let [first-prop-elt (get-naga-first st)]
      (let [remaining (:naga/rest st)
            [_ first-elt] (recurse-node store first-prop-elt)]
        (assert first-elt)
        ;; recursively build the list
        (if remaining
          (cons first-elt (build-list store (property-values store remaining)))
          (list first-elt))))))


(s/defn recurse-node :- s/Any
  "Determines if the val of a map entry is a node to be recursed on, and loads if necessary"
  [store :- StorageType
   [prop v :as prop-val] :- [s/Keyword s/Any]]
  (if-let [pairs (check-structure store prop v)]
    [prop (or (build-list store pairs)
              (pairs->json store pairs))]
    prop-val))


(s/defn pairs->json :- {s/Keyword s/Any}
  "Uses a set of property-value pairs to load up a nested data structure from the graph"
  [store :- StorageType
   prop-vals :- [[s/Keyword s/Any]]]
  (->> prop-vals
       (remove (comp #{:db/id :db/ident :naga/entity} first))
       (map (partial recurse-node store))
       (into {})))


(s/defn id->json :- {s/Keyword s/Any}
  "Uses an id node to load up a nested data structure from the graph"
  ([store :- StorageType
    entity-id :- s/Any]
   (id->json store entity-id nil))
  ([store :- StorageType
    entity-id :- s/Any
    exclusions :- (s/maybe #{s/Keyword})]
   (let [prop-vals (property-values store entity-id)
         pvs (if (seq exclusions)
               (remove (comp exclusions first) prop-vals)
               prop-vals)]
     (pairs->json store pvs))))


(s/defn ident->json :- {s/Keyword s/Any}
  "Converts data in a database to data structures suitable for JSON encoding"
  [store :- StorageType
   ident :- s/Any]
  ;; find the entity by its ident. Some systems will make the id the entity id,
  ;; and the ident will be separate, so look for both.
  (let [eid (or (ffirst (store/resolve-pattern store '[?eid :db/id ident]))
                (ffirst (store/resolve-pattern store '[?eid :db/ident ident])))]
    (id->json store eid)))

(s/defn store->json :- [{s/Keyword s/Any}]
  "Pulls all top level JSON out of a store"
  ([store :- StorageType]
   (store->json store nil))
  ([store :- StorageType
    exclusions :- (s/maybe #{s/Keyword})]
   (->> (store/query store '[?e] '[[?e :naga/entity true] [?e :db/ident ?id]])
        (map first)
        (map #(id->json store % exclusions)))))

(s/defn store->str :- s/Str
  "Reads a store into JSON strings"
  ([store :- StorageType]
   (json-generate-string (store->json store)))
  ([store :- StorageType, indent :- s/Num]
   (json-generate-string (store->json store) indent)))

(s/defn delta->json :- [{s/Keyword s/Any}]
  "Pulls all top level JSON out of a store"
  [store :- StorageType]
  (->> (store/deltas store)
       (map (partial id->json store))))

(s/defn delta->str :- s/Str
  "Reads a store into JSON strings"
  ([store :- StorageType]
   (json-generate-string (delta->json store)))
  ([store :- StorageType, indent :- s/Num]
   (json-generate-string (delta->json store) indent)))


;; updating the store

(s/defn existing-triples
  [storage id [k v]]
  (if-let [subpv (check-structure storage k v)]
    (cons [id k v] (mapcat (partial existing-triples storage v) subpv))
    [[id k v]]))

(s/defn json-update->triples :- [(s/one [Triple] "assertions") (s/one [Triple] "retractions")]
  "Takes a single structure and converts it into triples to be added and triples to be retracted to create a change"
  [storage id j]
  (binding [*current-storage* storage]
    (let [pvs (property-values storage id)
          old-node (pairs->json storage pvs)
          to-remove (remove (fn [[k v]] (if-let [newv (get j k)] (= v newv))) old-node)
          pvs-to-remove (filter (comp (set (map first to-remove)) first) pvs)
          triples-to-remove (mapcat (partial existing-triples storage id) pvs-to-remove)

          to-add (remove (fn [[k v]] (when-let [new-val (get old-node k)] (= new-val v))) j)
          triples-to-add (doall (mapcat (partial property-vals id) to-add))]
      [triples-to-add triples-to-remove])))
