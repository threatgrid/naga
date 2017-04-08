(ns ^{:doc "Storage wrapper around Datomic"
      :author "Paula Gearon"}
    naga.storage.datomic.core
  (:require [naga.store :as store]
            [naga.storage.datomic.init :as init]
            [naga.storage.datomic.schema :as sch]
            [naga.storage.store-util :as store-util]
            [naga.schema.structs :as ns]
            [schema.core :as s]
            [cheshire.core :as j]
            [naga.schema.structs :as st
                                 :refer [EPVPattern FilterPattern Pattern Results Value]]
            [clojure.string :as str]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [datomic.api :as d])
  (:import [naga.store Storage]
           [datomic.db DbId]
           [clojure.lang Symbol]
           [java.util Map List Date UUID]
           [java.io File IOException]
           [java.net URI]))

(def type->suffix {String "-s"
                   Long "-l"
                   Integer "-l"
                   Short "-l"
                   Byte "-l"
                   Boolean "-b"
                   Character "-c"
                   Date "-d"
                   UUID "-uu"
                   URI "-u"
                   List ""
                   Map ""})

(def type->dbtype {String :db.type/string
                   Long :db.type/long
                   Integer :db.type/long
                   Short :db.type/long
                   Byte :db.type/long
                   Boolean :db.type/boolean
                   Character :db.type/long
                   Date :db.type/instant
                   UUID :db.type/uuid
                   URI :db.type/uid
                   List :db.type/ref
                   Map :db.type/ref})

(defn kw-from-type*
  "Determines a keyword for a provided type"
  [t prefix]
  (let [suffix (type->suffix t)]
    (keyword "naga" (str prefix suffix))))

(def kw-from-type (memoize kw-from-type*))

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

(s/defn assertion-from-triple :- [(s/one s/Keyword "assertion")
                                  (s/one s/Any "entity")
                                  (s/one s/Keyword "attribute")
                                  (s/one s/Any "value")]
  "Converts a triple into a Datomic assertion"
  [attr->type->new :- {s/Keyword {s/Keyword s/Keyword}}
   [e a v] :- ns/Triple]
  (let [m (attr->type->new a)
        na (if m (m (type->dbtype (generic-type v)) :err/unscanned) a)]
    [:db/add e na v]))

(defn dvar? [v] (and (symbol? v) (= \? (first (name v)))))


(defrecord DatomicStore [connection db attributes tx-id log]
  Storage

  (start-tx [this]
    (if tx-id
      ;; already in a transaction. Don't start a new one
      this

      ;; record the current database and transaction
      (let [latest-db (d/db connection)
            latest-log (d/log connection)
            latest-tx (tx latest-db)]
        (->DatomicStore connection latest-db attributes latest-tx log))))

  (commit-tx [this]
          ;; retrieve all the assertions after the recorded transaction
    (let [data (mapcat :data (d/tx-range log (inc tx-id) nil))
          ;; replay those assertions into the database
          {db-after :dbafter} @(d/transact connection data)]
      ;; return the new state of the database
      (->DatomicStore connection db-after attributes nil nil)))

  (new-node [_]
    (d/tempid :naga/data))  ;; this matches the partition in init/pre-init-data

  (node-id [_ n]
    (subs (str (:idx n)) 1))

  (node-type? [_ prop value]
    (instance? DbId value))
  
  (data-property [_ data]
    (kw-from-type (generic-type data) "first"))

  (container-property [_ data]
    (kw-from-type (generic-type data) "contains"))

  (resolve-pattern [this pattern]
    (let [vars (filter symbol? pattern)]
      (d/q {:find vars :where [pattern]} db)))

  (count-pattern [this pattern]
    (if-let [[fvar & rvars] (seq (filter dvar? pattern))]
      (d/q {:find [(list 'count fvar) '.]
            :with rvars
            :where [pattern]} db)
      (let [[e a v] pattern]  ;; existence test: 0 or 1
        (d/q {:find '[(count ?a) .]
              :where [[e '?a v] '[?a :db/ident]]} db))))
  
  (query [this output-pattern patterns]
    (let [vars (filter symbol? output-pattern)
          ;; query may have constants, which are not supported by Datomic
          ;; so these must be projected into the result
          project-output (if (= vars output-pattern)
                           identity
                           (partial store-util/project
                                    this
                                    (vec output-pattern)))]
      (project-output
        (d/q {:find vars :where patterns} db))))

  (assert-data [_ data]
    ;; if in a transaction, speculatively add data to the current database
    ;; otherwise insert normally
    (let [tx-fn (if tx-id
                  (partial d/with (or db (d/db connection)))
                  (comp deref (partial d/transact connection)))
          datomic-assertions (map (partial assertion-from-triple attributes) data)
          _ (println "ASSERTIONS: " datomic-assertions)
          {db-after :db-after :as result} (tx-fn datomic-assertions)]
      ;; return the new state. Note the TX ID and log do not change,
      ;; as these have the replay point, if in a transaction.
      (->DatomicStore connection db-after attributes tx-id log)))

  (query-insert [this assertion-patterns patterns]
    ;; compose from query/assert-data
    (let [get-vars (partial mapcat (partial filter dvar?))
          simple-project-pattern (get-vars assertion-patterns)
          project-pattern (if (seq simple-project-pattern)
                            simple-project-pattern
                            [(first (get-vars patterns))])]
      (->> (store/query this project-pattern patterns)
           (store-util/insert-project this assertion-patterns project-pattern)
           (store/assert-data this)))))


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

(s/defn file-type :- s/Keyword
  "Guesses at the type of file provided"
  [file]
  (letfn [(ext [s]
            (if-let [i (str/last-index-of s \.)]
              (subs s (inc i))))]
    ({"json" :json
      "js" :json
      "edn" :edn
      "type" :pairs
      "pair" :pairs} (ext (.getName file)))))

(s/defn user-init
  "Initializes the database with user data"
  [connection user-data-file :- s/Str]
  (when user-data-file
    (let [file (File. user-data-file)]
      (when (.exists file)
        (let [data (try
                     (case (file-type file)
                       :json (sch/auto-schema (j/parse-stream (io/reader file)))

                       :pairs (sch/pair-file-to-attributes (slurp file))

                       :edn (edn/read-string (slurp file))

                       (throw (ex-info "Unable to determine initialization file type"
                                       {:file user-data-file})))
                     (catch IOException e
                       (throw (ex-info "Unable to read initialization file"
                                       {:file user-data-file :ex e}))))]
          (d/transact connection data))))))

(s/defn read-attribute-types
  [db]
  (let [attr-tuples (d/q '[:find ?onm ?tn ?anm
                           :where
                           [?a :naga/original ?oid]
                           [?a :db/ident ?anm]
                           [?a :db/valueType ?t]
                           [?t :db/ident ?tn]
                           [?oid :db/ident ?onm]]
                         db)
        groupings (group-by first attr-tuples)]
    (->> groupings
         (map (fn [[o s]]
                [o (into {} (map (comp vec rest) s))]))
         (into {}))))

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
        db (d/db connection)
        attr (read-attribute-types db)]
    (->DatomicStore connection db attr nil nil)))

(store/register-storage! :datomic create-store)
