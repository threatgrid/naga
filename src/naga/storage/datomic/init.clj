(ns ^{:doc "Initialization data for Datomic"
      :author "Paula Gearon"}
    naga.storage.datomic.init
  (:require [clojure.string :as s]
            [naga.util :as u]
            [datomic.api :as d :refer [q]])
  (:import [datomic Peer]
           [java.util Map List]))

(def internal-attributes
  [{:db/id (Peer/tempid :db.part/db)
    :db/ident :naga/rest
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/install/_attribute :db.part/db}
   {:db/id (Peer/tempid :db.part/db)
    :db/ident :naga/original
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/install/_attribute :db.part/db}
   {:db/id (Peer/tempid :db.part/db)
    :db/ident :naga/json.type
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/install/_attribute :db.part/db}
   {:db/id (Peer/tempid :db.part/db)
    :db/ident :naga/attributes
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many
    :db/install/_attribute :db.part/db}
   {:db/id (Peer/tempid :db.part/db)
    :db/ident :map}
   {:db/id (Peer/tempid :db.part/db)
    :db/ident :array}])

(defn value-property-decl
  [domain [vtype ext]]
  (let [prop (keyword "naga" (str domain ext))
        declaration {:db/id (Peer/tempid :db.part/db)
                     :db/ident prop
                     :db/valueType vtype
                     :db/cardinality :db.cardinality/one
                     :db.install/_attribute :db.part/db}]
    (if (= vtype :db.type/ref)
      (assoc declaration :db/isComponent true)
      declaration)))

(def type-properties
  {:db.type/ref ""
   :db.type/string "-s"
   :db.type/boolean "-b"
   :db.type/long "-l"
   :db.type/bigint "-bi"
   :db.type/float "-f"
   :db.type/double "-d"
   :db.type/bigdec "-bd"
   :db.type/instant "-dt"
   :db.type/uuid "-uu"
   :db.type/uri "-u"})

(def init-attributes (concat
                      (map (partial value-property-decl "first") type-properties)
                      (map (partial value-property-decl "contains") type-properties)
                      internal-attributes))

(defn initializing-data
  "Returns initializing data for any attributes that are not in the database."
  [db]
  (let [system-attributes (map :db/ident init-attributes)
        existing-attributes (set (q '[:find [?id ...] :in $ [?a ...] :where [?id :db/ident ?a]]
                                    db system-attributes))]
    (remove (comp existing-attributes :db/ident) init-attributes)))


(def pre-init-data
  [{:db/id "naga-data"
    :db/ident :naga/data}
   [:db/add :db.part/db :db.install/partition "naga-data"]])

(defn- convert-typename
  "Ensures that user-friendly typenames are "
  [t]
  ({"map" ["ref" :map]
    "array" ["ref" :array]} t [t nil]))

(defn- attribute-data
  "Generates data for new attribute definitions, based on a sequence of name/type string pairs"
  [pairs]
  (let [grouped (into [] (map (fn [[k v]] [k (vec (set v))]) (group-by first pairs)))
        [simple-pairs complex-pairs] (u/divide #(= 1 (count (second %))) grouped)
        simple-def (fn [[_ [[nm t]]]]
                     (let [[tp ot] (convert-typename t)
                           sch {:db/id (Peer/tempid :db.part/db)
                                :db/ident (keyword nm)
                                :db/valueType (keyword "db.type" tp)
                                :db/cardinality :db.cardinality/one
                                :db.install/_attribute :db.part/db}]
                       (if ot (assoc sch :naga/json.type ot) sch)))
        complex-def (fn [[nm ps]]
                      (let [nk (keyword nm)
                            attributes (map
                                        (fn [[_ t]]
                                          (let [[tp ot] (convert-typename t)
                                                sch {:db/id (Peer/tempid :db.part/db)
                                                     :db/ident (keyword (str nm "." tp))
                                                     :db/valueType (keyword "db.type" tp)
                                                     :db/cardinality :db.cardinality/one
                                                     :naga/original nk
                                                     :db.install/_attribute :db.part/db}]
                                            (if ot (assoc sch :naga/json.type ot) sch)))
                                        ps)]
                        (cons
                         {:db/id (Peer/tempid :db.part/db)
                          :db/ident nk
                          :naga/attributes (map :db/ident attributes)}
                         attributes)))]
    (concat
     (map simple-def simple-pairs)
     (mapcat complex-def complex-pairs))))

(defn pair-file-to-attributes
  "Generates data for new attribute definitions, based on a file of attribute/type pairs."
  [file-path]
  (->> (s/split (slurp file-path) #"\n")
       (map #(s/split % #"\W+"))
       attribute-data))

(defprotocol Dataschema
  (typename [data] "Returns the name of a JSON type")
  (schema-from [data xfr] "Generate a schema out of data, and add to the xfr transactor"))

(def special-cases
  {"integer" "long"})

(extend-protocol Dataschema
  Map
  (typename [jmap] "map")
  (schema-from [jmap xfr]
    (doseq [[k v] jmap]
      (if-let [t (typename v)]
        (xfr [(name k) t]))
      (schema-from v xfr)))
  List
  (typename [jlist] "array")
  (schema-from [jlist xfr]
    (doseq [l jlist] (schema-from l xfr)))
  nil
  (typename [n])
  (schema-from [n xfr])
  Object
  (typename [jdata]
    (let [tn (s/lower-case (.getSimpleName (class jdata)))]
      (special-cases tn tn)))
  (schema-from [data xfr]))

(defn extract-types
  "Return a sequence of property/type pairs identified in JSON.
   JSON must be a sequence of entity maps."
  [json-data]
  (let [tx (fn [xf]
             (fn
               ([] (xf))
               ([result] (xf result))
               ([result input]
                 (schema-from input (partial xf result)))))]
    (when-not (sequential? json-data)
      (throw (ex-info "Invalid JSON sequence" {:data json-data})))
    (sequence tx json-data)))

(defn auto-schema
  "Determine a Datomic schema for a provided JSON structure."
  [json-data]
  (attribute-data (extract-types json-data)))
