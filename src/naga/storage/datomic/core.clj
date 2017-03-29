(ns ^{:doc "Storage wrapper around Datomic"
      :author "Paula Gearon"}
    naga.storage.datomic.core
  (:require [naga.store :as store]
            [naga.storage.datomic.init :as init]
            [schema.core :as s]
            [naga.schema.structs :as st
                                 :refer [EPVPattern FilterPattern Pattern Results Value]]
            [clojure.string :as str]
            [clojure.edn :as edn]
            [datomic.api :as d])
  (:import [naga.store Storage]
           [datomic.db DbId]
           [clojure.lang Symbol]
           [java.util Map List Date UUID]
           [java.io File]
           [java.net URI]))

(def type->first {String :naga/first-s
                  Long :naga/first-l
                  Integer :naga/first-i
                  Short :naga/first-s
                  Byte :naga/first-b
                  Character :naga/first-c
                  Date :naga/first-d
                  UUID :naga/first-uu
                  URI :naga/first-u
                  List :naga/first
                  Map :naga/first})

(def type->contains {String :naga/contains-s
                     Long :naga/contains-l
                     Integer :naga/contains-i
                     Short :naga/contains-s
                     Byte :naga/contains-b
                     Character :naga/contains-c
                     Date :naga/contains-d
                     UUID :naga/contains-uu
                     URI :naga/contains-u
                     List :naga/contains
                     Map :naga/contains})

(defn generic-type
  "Determines a general data type for the given data"
  [d]
  (cond
    (instance? Map d) Map
    (instance? List d) List
    :default (class d)))

(s/defn project-row :- [s/Any]
  "Creates a new EPVPattern from an existing one, based on existing bindings.
   Uses the mapping to copy from columns in 'row' to overwrite variables in 'pattern'.
   'pattern' must be a vector.
   The index mappings have already been found and are in the 'mapping' argument"
  [pattern :- EPVPattern
   mapping :- [[s/Num s/Num]]
   row :- [Value]]
  (reduce (fn [p [f t]] (assoc p t (nth row f))) pattern mapping))

(s/defn matching-vars :- [[s/Num s/Num]]
  "Returns pairs of indexes into seqs where the vars match.
   For any variable that appears in both sequences, the column number in the
   'from' parameter gets mapped to the column number of the same variable
   in the 'to' parameter."
  [to :- [s/Any]
   from :- [Symbol]]
  (->> from
       (map-indexed
        (fn [nt vt]
          (seq
           (map-indexed
            (fn [nf vf]
              (if (and (st/vartest? vf) (= vt vf))
                [nf nt]))
            to))))
       (apply concat)))

(defn tx
  "Determines the transaction ID for a database"
  [db]
  (d/t->tx (d/basis-t db)))



(defrecord DatomicStore [connection db tx-id log]
  Storage

  (start-tx [this]
    (if tx-id
      ;; already in a transaction. Don't start a new one
      this

      ;; record the current database and transaction
      (let [latest-db (d/db connection)
            latest-log (d/log connection)
            latest-tx (tx latest-db)]
        (->DatomicStore connection latest-db latest-tx log))))

  (commit-tx [this]
          ;; retrieve all the assertions after the recorded transaction
    (let [data (mapcat :data (d/tx-range log (inc tx-id) nil))
          ;; replay those assertions into the database
          {db-after :dbafter} @(d/transact connection data)]
      ;; return the new state of the database
      (->DatomicStore connection db-after nil nil)))

  (new-node [_]
    (d/tempid :naga/data))  ;; this matches the partition in init/pre-init-data

  (node-id [_ n]
    (subs (str (:idx n)) 1))

  (node-type? [_ prop value]
    (instance? DbId value))
  
  (data-property [_ data]
    (type->first (generic-type data)))

  (container-property [_ data]
    (type->contains (generic-type data)))

  (resolve-pattern [_ pattern]
    (let [vars (filter symbol? pattern)]
      (d/q {:find vars :where pattern}) db))

  (query [_ output-pattern patterns]
    (let [vars (filter symbol? output-pattern)
          ;; query may have constants, which are not supported by Datomic
          ;; so these must be projected into the result
          project-output (if (= vars output-pattern)
                           identity
                           (partial project-row
                                    (vec output-pattern)
                                    (matching-vars output-pattern vars)))]
      (map project-output
           (d/q {:find vars :where patterns} db))))

  (assert-data [_ data]
    ;; if in a transaction, speculatively add data to the current database
    ;; otherwise insert normally
    (let [tx-fn (if tx-id
                  (partial d/with (or db (d/db connection)))
                  (partial d/transact connection))
          datomic-assertions (map (partial cons :db/add) data)
          {db-after :dbafter} @(tx-fn datomic-assertions)]
      ;; return the new state. Note the TX ID and log do not change,
      ;; as these have the replay point, if in a transaction.
      (->DatomicStore connection db-after tx-id log)))

  (query-insert [this assertion-pattern patterns]
    ;; compose from query/assert-data
    (store/assert-data this (store/query this assertion-pattern patterns))))


(s/defn build-uri :- String
  "Reads a configuration map, or creates a Datomic URI. Reports an error if both are not valid"
  [uri :- URI
   m :- String]
  (if m
    (try
      (edn/read-string m)
      (catch Exception _ (build-uri uri nil)))
    (let [uri-str (str uri)]
      (when (> 2 (count (str/split uri-str #"://")))
        (throw (ex-info (str "Invalid Datomic URI: " uri-str) {:uri uri-str})))
      (if (str/starts-with? uri-str "datomic:")
        uri-str
        (str "datomic:" uri-str)))))

(s/defn init
  "Initializes storage, and returns the result of any transaction. Returns nil if no transaction was needed."
  [connection]
  (d/transact connection init/pre-init-data)
  (let [db (d/db connection)
        tx-data (init/initializing-data db)]
    (when (seq tx-data)
      (d/transact connection tx-data))))

(s/defn load-from-path
  "Silently loads a path, or returns nil"
  [path]
  (when (and path (.exists (File. path)))
    (try (slurp path) (catch Exception _))))

(s/defn user-init
  "Initializes the database with user data"
  ;; TODO: this naively reads edn. Load pairs or use auto-schema.
  [connection user-data-file]
  (if-let [data-text (load-from-path user-data-file)]
    (let [data (edn/read-string data-text)]
      (d/transact connection data))))

(s/defn create-store :- Storage
  "Factory function to create a store"
  [{uri :uri user-data :init mp :map :as config}]
  (let [uri (build-uri uri mp)
        connection (if (d/create-database uri)
                     (let [conn (d/connect uri)]
                       (init conn)
                       conn)
                     (d/connect uri))
        _ (user-init connection user-data)
        db (d/db connection)]
    (->DatomicStore connection db nil nil)))

(store/register-storage! :datomic create-store)
