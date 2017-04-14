(ns ^{:doc "Initialization data for Datomic"
      :author "Paula Gearon"}
    naga.storage.datomic.init
  (:require [datomic.api :as d :refer [q]])
  (:import [datomic Peer]))

(def internal-attributes
  [{:db/id (Peer/tempid :db.part/db)
    :db/ident :naga/rest
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (Peer/tempid :db.part/db)
    :db/ident :naga/original
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (Peer/tempid :db.part/db)
    :db/ident :naga/json.type
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (Peer/tempid :db.part/db)
    :db/ident :naga/attributes
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many
    :db.install/_attribute :db.part/db}
   {:db/id (Peer/tempid :db.part/db)
    :db/ident :naga/json.entity
    :db/valueType :db.type/boolean
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (Peer/tempid :db.part/db)
    :db/ident :object}
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

(def init-attributes
  (concat
   (map (partial value-property-decl "first") type-properties)
   (map (partial value-property-decl "contains") type-properties)
   internal-attributes))

(defn initializing-data
  "Returns initializing data for any attributes that are not in the database."
  [db]
  (let [system-attributes (map :db/ident init-attributes)
        existing-attributes (set (q '[:find [?id ...]
                                      :in $ [?a ...]
                                      :where [?id :db/ident ?a]]
                                    db system-attributes))]
    (remove (comp existing-attributes :db/ident) init-attributes)))


(def pre-init-data
  [{:db/id "naga-data"
    :db/ident :naga/data}
   [:db/add :db.part/db :db.install/partition "naga-data"]])

