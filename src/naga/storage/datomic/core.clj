(ns ^{:doc "Storage wrapper around Datomic"
      :author "Paula Gearon"}
    naga.storage.datomic.core
  (:require [naga.store :as store]
            [naga.store-registry :as store-registry]
            [naga.storage.datomic.init :as init]
            [naga.storage.datomic.schema :as sch]
            [naga.storage.store-util :as store-util]
            [naga.schema.store-structs :as nss]
            [naga.util :as u]
            [schema.core :as s]
            [cheshire.core :as j]
            [naga.schema.store-structs :as ss
                                 :refer [EPVPattern FilterPattern Pattern Results Value]]
            [clojure.string :as str]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [datomic.api :as d :refer [q]])
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
              (if (and (ss/vartest? vf) (= vt vf))
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
   [e a v] :- nss/Triple]
  (let [m (attr->type->new a)
        na (if m (m (type->dbtype (generic-type v)) :err/unscanned) a)]
    [:db/add e na v]))

(defn dvar? [v] (and (symbol? v) (= \? (first (name v)))))

(defn expand-symbol
  "Converts a pattern with a variable attribute into a pair
  that converts the requested attribute into its identifier.
  Returns a seq of patterns."
  [sym-test? [e a v :as pattern]]
  (if (sym-test? a)
    (let [ae (symbol (str (name a) ".x"))]
      [[e ae v] [ae :db/ident a]])
    [pattern]))

(defn get-attrib-projection
  "Returns a projection function, given a pattern and the current attributes."
  [{o :originals} [_ attr _ :as p]]
  (if (symbol? attr)
    (let [vars (filter symbol? p)
          aindexes (set (keep-indexed (fn [i v] (when (= v attr) i)) vars))]
      (fn [results]
        (map
         (fn [row]
           (vec (map-indexed (fn [i v] (if (aindexes i) (o v v) v)) row)))
         results)))
    identity))

(s/defn read-attribute-info
  :- {(s/required-key :overloads) {s/Keyword {s/Keyword s/Keyword}}
      (s/required-key :originals) {s/Keyword s/Keyword}
      (s/required-key :types) {s/Keyword s/Keyword}}
  "Reads attribute info and uses this to create 3 maps.
  1. Maps overloaded attributes to a map of type->name,
     where the name is the attribute to use for that type.
  2. Maps aliases for the overloaded attribute back to the original.
  3. Maps attribute names to the type of data they hold."
  [db]
  (let [attr-tuples (q '[:find ?onm ?tn ?anm
                         :where
                         [?a :naga/original ?oid]
                         [?a :db/ident ?anm]
                         [?a :db/valueType ?t]
                         [?t :db/ident ?tn]
                         [?oid :db/ident ?onm]]
                       db)
        overloads (->> (group-by first attr-tuples)
                       (map (fn [[o s]]
                              [o (into {} (map (comp vec rest) s))]))
                       (into {}))
        originals (into {} (map (fn [[o _ a]] [a o]) attr-tuples))
        attr-types (into {} (q '[:find ?anm ?tn
                                 :where
                                 [?a :db/ident ?anm]
                                 [?a :db/valueType ?t]
                                 [?t :db/ident ?tn]]
                               db))]
    {:overloads overloads
     :originals originals
     :types attr-types}))


(defn top-ids
  "Adds to an accumulator all Database entities that represent a top level entity"
  [db ids acc]
  (let [eids (q '[:find [?i ...]
                  :in $ [?i ...]
                  :where [?i :naga/entity]] db ids)
        eids? (into #{} eids) 
        parented-ids (remove eids? ids)
        pids (q '[:find [?pid ...]
                  :in $ [?i ...]
                  :where [?pid ?a ?i]] db parented-ids)
        pids' (remove (into #{} parented-ids) pids)] ;; remove potential loops
    (if (seq pids')
      (recur db pids' (concat acc eids))
      acc)))


(defn transaction-fn
  "Create a transaction function for the given storage configuration"
  [{:keys [tx-id db connection]}]
  (comp :db-after
        (if tx-id
          (partial d/with (or db (d/db connection)))
          (comp deref (partial d/transact connection)))))

;; TODO: alias projection
;; if any attribute in a query patterns is a symbol, AND that symbol is in the output
;; then update the projection to rewrite that symbol as per get-attrib-projection

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
        (->DatomicStore connection latest-db attributes latest-tx latest-log))))

  (commit-tx [this]
    ;; retrieve all the assertions after the recorded transaction
    (let [new-entities (q [:find '[?e ...] :where '[?e :db/ident _ ?t] [(list '> '?t tx-id)]] db)
          entity-map (u/mapmap (fn [x] (d/tempid :naga/data)) new-entities)
          data (map (fn [[e a v]] [:db/add (entity-map e e) a v])
                    (q [:find '?e '?a '?v
                        :where '[?e ?ax ?v ?t]
                        '(not [?e :db/txInstant])
                        '[?ax :db/ident ?a]
                        [(list '> '?t tx-id)]]
                       db))
          ;; replay those assertions into the database
          {db-after :db-after} @(d/transact connection data)]
      ;; return the new state of the database, add the option of data as metadata
      (with-meta (->DatomicStore connection db-after attributes nil nil) {:data data})))

  (deltas [this]
    (when-let [{data :data} (meta this)]
      (let [ids (map first data)]
        (top-ids db ids []))))

  (new-node [_]
    (d/tempid :naga/data)) ;; this matches the partition in init/pre-init-data

  (node-id [_ n]
    (subs (str (:idx n)) 1))

  (node-type? [_ prop value]
    ;; NB: an aliased property which can be a Long may incorrectly return true when value is Long.
    ;; Rebuilding the structure will identify that the long does not refer to actual data.
    (or (instance? DbId value)
        (if-let [at (get (:types attributes) prop)] ;; look for the attribute type
          ;; is the attribute a ref?
          (= :db.type/ref at)
          ;; attribute not known. Therefore aliased
          (and (:db.type/ref (get (:overloads attributes) prop)) ;; is ref possible?
               (instance? Long value))))) ;; ensure it's compatible with ref
  
  (data-property [_ data]
    (kw-from-type (generic-type data) "first"))

  (container-property [_ data]
    (kw-from-type (generic-type data) "contains"))

  (resolve-pattern [this pattern]
    (let [vars (filter symbol? pattern)
          patterns (expand-symbol symbol? pattern)
          aproject (get-attrib-projection attributes pattern)]
      (aproject (q {:find vars :where patterns} db))))

  (count-pattern [this pattern]
    (let [[fvar & rvars] (seq (filter dvar? pattern))]
      (if (seq (remove dvar? pattern))
        (q {:find [(list 'count fvar) '.]
            :with rvars
            :where [pattern]} db)
        (let [[e a v] pattern] ;; existence test: 0 or 1
          (q {:find '[(count ?a) .]
              :where [[e '?a v] '[?a :db/ident]]} db)))))
  
  (query [this output-pattern patterns]
    ;; TODO: re-project output for aliases. Low priority: queries are rare.
    (let [vars (filter symbol? output-pattern)
          ;; query may have constants, which are not supported by Datomic
          ;; so these must be projected into the result
          project-output (if (= vars output-pattern)
                           identity
                           (partial store-util/project
                                    this
                                    output-pattern))
          symbol-expansion (partial expand-symbol (set vars))
          patterns (mapcat symbol-expansion patterns)]
      (project-output
       (q {:find vars :where patterns} db))))

  (assert-data [this data]
    ;; if in a transaction, speculatively add data to the current database
    ;; otherwise insert normally
    (let [build-assertion (partial assertion-from-triple
                                   (:overloads attributes))
          datomic-assertions (map build-assertion data)
          db-after ((transaction-fn this) datomic-assertions)]
      ;; return the new state. Note the TX ID and log do not change,
      ;; as these have the replay point, if in a transaction.
      (->DatomicStore connection db-after attributes tx-id log)))

  (assert-schema-opts [this schema-data {stype :type :as opts}]
    (let [schema (case stype
                   :json (sch/auto-schema schema-data)
                   :pairs (sch/pair-file-to-attributes schema-data)
                   :edn schema-data
                   (throw (ex-info "Unknown schema type: " opts)))
          db-after ((transaction-fn this) schema)
          attr (read-attribute-info db-after)]
      (->DatomicStore connection db-after attr tx-id log)))

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

(s/defn init!
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
      "pair" :pairs} (str/lower-case (ext (.getName file))))))

(s/defn user-init-data
  "Generates initialization transaction data from user data"
  [user-data-file :- s/Str]
  (when user-data-file
    (let [file (File. user-data-file)]
      (when (.exists file)
        (try
          (case (file-type file)
            :json (sch/auto-schema (j/parse-stream (io/reader file)))

            :pairs (sch/pair-file-to-attributes (slurp file))

            :edn (edn/read-string (slurp file))

            (throw (ex-info "Unable to determine initialization file type"
                            {:file user-data-file})))
          (catch IOException e
            (throw (ex-info "Unable to read initialization file"
                            {:file user-data-file :ex e}))))))))

(s/defn create-store :- (s/pred #(extends? Storage (class %)))
  "Factory function to create a store"
  [{uri :uri user-data :init data-file :json mp :map :as config}]
  (let [uri (build-uri uri mp)
        _ (d/create-database uri)
        connection (d/connect uri)
        _ (init! connection)
        init-data (user-init-data user-data)
        file-schema (user-init-data data-file)
        _ (when-let [initial-tx (seq (concat init-data file-schema))]
            @(d/transact connection initial-tx))
        db (d/db connection)
        attr (read-attribute-info db)]
    (->DatomicStore connection db attr nil nil)))

(store-registry/register-storage! :datomic create-store #(d/shutdown true))
