(ns ^{:doc "Functions for generating a user schema for Datomic."
      :author "Paula Gearon"}
    naga.storage.datomic.schema
  (:require [clojure.string :as s]
            [naga.util :as u])
  (:import [datomic Peer]
           [java.util Map List]))


(defn- convert-typename
  "Ensures that user-friendly typenames are "
  [t]
  ({"map" ["ref" :object]
    "object" ["ref" :object]
    "entity" ["ref" :object]
    "array" ["ref" :array]} t [t nil]))

(defn- simple-def
  [[_ [[nm t]]]]
  (let [[tp ot] (convert-typename t)
        sch {:db/id (Peer/tempid :db.part/db)
             :db/ident (keyword nm)
             :db/valueType (keyword "db.type" tp)
             :db/cardinality :db.cardinality/one
             :db.install/_attribute :db.part/db}]
    (if ot (assoc sch :naga/json.type ot) sch)))

(defn- complex-def
  [[nm ps]]
  (let [nk-id (Peer/tempid :db.part/db)
        attributes (map
                    (fn [[_ t]]
                      (let [[tp ot] (convert-typename t)
                            sch {:db/id (Peer/tempid :db.part/db)
                                 :db/ident (keyword (str nm "." tp))
                                 :db/valueType (keyword "db.type" tp)
                                 :db/cardinality :db.cardinality/one
                                 :naga/original nk-id
                                 :db.install/_attribute :db.part/db}]
                        (if ot (assoc sch :naga/json.type ot) sch)))
                    ps)]
    (cons
     {:db/id nk-id
      :db/ident (keyword nm)
      :naga/attributes (map :db/id attributes)}
     attributes)))

(defn- attribute-data
  "Generates data for new attribute definitions, based on a sequence of name/type string pairs.
   Returns a sequence of transaction data sequences, which will need to be transacted in order."
  [pairs]
  (let [grouped (vec (map (fn [[k v]] [k (vec (set v))]) (group-by first pairs)))
        [simple-pairs complex-pairs] (u/divide #(= 1 (count (second %))) grouped)]
    (concat
     (map simple-def simple-pairs)
     (mapcat complex-def complex-pairs))))

(defn pair-file-to-attributes
  "Generates data for new attribute definitions, based on a file of attribute/type pairs."
  [file-text]
  (->> (s/split file-text #"\n")
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
