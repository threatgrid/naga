(ns ^{:doc "A storage implementation over in-memory indexing. Includes full query engine."
      :author "Paula Gearon"}
    naga.storage.asami.core
    (:require [clojure.string :as str]
              [asami.core :as asami]
              [asami.graph :as gr]
              [asami.storage :as asami-storage]
              #?(:clj [asami.memory]
                 :cljs [asami.memory :refer [MemoryConnection]])
              [asami.index :as mem]
              [asami.multi-graph :as multi]
              [asami.query :as query]
              [asami.internal :as internal]
              [naga.store :as store :refer [Storage StorageType ConnectionStore]]
              [zuko.projection :as projection]
              [naga.store-registry :as registry]
              #?(:clj  [schema.core :as s]
                 :cljs [schema.core :as s :include-macros true])
              #?(:clj [clojure.core.cache :as c]))
    #?(:clj
       (:import [asami.memory MemoryConnection])))


#?(:clj
  ;; Using a cache of 1 is currently redundant to an atom
  (let [m (atom (c/lru-cache-factory {} :threshold 1))]
    (defn get-count-fn
      "Returns a memoized counting function for the current graph.
       These functions only last as long as the current graph."
      [graph]
      (if-let [f (c/lookup @m graph)]
        (do
          (swap! m c/hit graph)
          f)
        (let [f (memoize #(gr/count-pattern graph %))]
          (swap! m c/miss graph f)
          f))))

  :cljs
  (let [m (atom {})]
    (defn get-count-fn
      "Returns a memoized counting function for the current graph.
       These functions only last as long as the current graph."
      [graph]
      (if-let [f (get @m graph)]
        f
        (let [f (memoize #(gr/count-pattern graph %))]
          (reset! m {graph f})
          f)))))

(defn shorten
  "truncates a symbol or keyword to exclude a ' character"
  [a]
  (if (string? a)
    a
    (let [nns (namespace a)
          n (name a)
          cns (cond
                (symbol? a) symbol
                (keyword? a) keyword
                :default (throw (ex-info "Invalid attribute type in rule head" {:attribute a :type (type a)})))]
      (cns nns (subs n 0 (dec (count n)))))))

(declare ->AsamiStore)

(def ^:const node-name-len (count "node-"))

(defn next-tx
  "The next transaction id that a connection should use.
  TODO: Update connections to support this internally."
  [c]
  (count (:history c)))

(defn data-update
  "Perform a data update on a graph or graph in a connection, depending on transaction state"
  [{:keys [connection graph tx-id] :as store} additions removals]
  (let [new-graph (if tx-id
                    ;; in a transaction, so update the graph without telling the connection
                    (gr/graph-transact graph tx-id additions removals)
                    ;; not in a transaction, so use the connection to do the update
                    (-> (asami-storage/transact-data connection additions removals)
                        second
                        asami/graph))]
    (assoc store :graph new-graph)))

(defrecord AsamiStore [connection before-graph graph tx-id]
  Storage
  (start-tx [this]
    (let [latest-graph (asami/graph (asami/db connection))]
      (->AsamiStore connection latest-graph latest-graph (next-tx connection))))

  (commit-tx [this]
    (let [ctx (next-tx connection)]
      (if (= tx-id ctx)
        ;; This operation mutates the connection
        (let [[_ db-after] (asami-storage/transact-update connection (fn [g t] graph))]
          (->AsamiStore connection before-graph (asami/graph db-after) nil))
        (throw (ex-info "Concurrent modification exception. Rolling back."
                        {:tx-id tx-id :connection-tx ctx})))))

  (deltas [this]
    ;; sort responses by the number in the node ID, since these are known to be ordered
    (when-let [previous-graph (or (:data (meta this)) before-graph)]
      (->> (gr/graph-diff graph previous-graph)
           (filter (fn [s] (seq (gr/resolve-pattern graph [s :tg/entity '?]))))
           (sort-by #(subs (name %) node-name-len)))))

  (count-pattern [_ pattern]
    (if-let [count-fn (get-count-fn graph)]
      (count-fn pattern)
      (gr/count-pattern graph pattern)))

  (query [this output-pattern patterns]
    (projection/project internal/project-args
                        output-pattern
                        (query/join-patterns graph patterns nil {})))

  (assert-data [this data]
    (data-update this data nil))

  (retract-data [this data]
    (data-update this nil data))

  (assert-schema-opts [this _ _] this)

  (resolve-pattern [_ pattern]
    (gr/resolve-pattern graph pattern))

  (query-insert [this assertion-patterns patterns]
    ;; convert projection patterns to output form
    ;; remember which patterns were converted
    (let [[assertion-patterns'
           update-attributes] (reduce (fn [[pts upd] [e a v :as p]]
                                        (if (or (str/ends-with? (name a) "'") (:update (meta p)))
                                          (let [short-a (shorten a)]
                                            [(conj pts [e short-a v]) (conj upd short-a)])
                                          [(conj pts p) upd]))
           [[] #{}]
           assertion-patterns)
          var-updates (set (filter symbol? update-attributes))
          ins-project (fn [data]
                        (let [cols (:cols (meta data))]
                          (if (seq var-updates)
                            (throw (ex-info "Updating variable attributes not yet supported" {:vars var-updates}))
                            ;; TODO: when insert-project is imported, modify to attach columns for update vars 
                            (projection/insert-project (internal/project-ins-args graph)
                                                       assertion-patterns' var-updates cols data))))
          lookup-triple (fn [part-pattern]
                          (let [pattern (conj part-pattern '?v)
                                values (gr/resolve-pattern graph pattern)]
                            (sequence (comp (map first) (map (partial conj part-pattern))) values)))
          is-update? #(update-attributes (nth % 1))
          addition-bindings (ins-project (query/join-patterns graph patterns nil {}))
          removals (->> addition-bindings
                        (filter is-update?)
                        (map #(vec (take 2 %)))
                        (mapcat lookup-triple))
          additions (if (seq var-updates) (map (partial take 3) addition-bindings) addition-bindings)]
      (data-update this additions removals))))

(defn update-store	
  "Note: This is currently employed by legacy code that is unaware of transaction IDs.
  Consider using the Asami API directly."
  [{:keys [connection before-graph tx-id]} f & args]	
  (when tx-id
    (throw (ex-info "Illegal State. May not update a graph during a transaction." {:uri (:name connection)})))
  (let [{:keys [db-after]} @(asami/transact connection {:update-fn (fn [g tx] (apply f g args))})]
    (->AsamiStore connection before-graph (asami/graph db-after) nil)))

(s/defn create-store :- StorageType
  "Factory function to create a store"
  ([] (create-store nil))
  ([{:keys [uri] :as config}]
   (if uri
     (->AsamiStore (asami/as-connection mem/empty-graph uri) nil mem/empty-graph nil)
     (->AsamiStore (asami/as-connection mem/empty-graph) nil mem/empty-graph nil))))

(s/defn create-multi-store :- StorageType
  "Factory function to create a multi-graph-store"
  ([] (create-multi-store nil))
  ([{:keys [uri] :as config}]
   (if uri
     (->AsamiStore (asami/as-connection multi/empty-multi-graph uri) nil multi/empty-multi-graph nil)
     (->AsamiStore (asami/as-connection multi/empty-multi-graph) nil multi/empty-multi-graph nil))))

(registry/register-storage! :memory create-store)
(registry/register-storage! :asami create-store)
(registry/register-storage! :memory-multi create-multi-store)

(s/defn graph->store :- StorageType
  "Wraps a graph in the Storage record"
  [graph :- gr/GraphType]
  (->AsamiStore (asami/as-connection graph) nil graph nil))

(extend-type MemoryConnection
  ConnectionStore
  (as-store [c] (->AsamiStore c nil (asami/graph (asami/db c)) nil)))

;; TODO: Add the durable connection type
;; (extend-type DurableConnection
;;  ConnectionStore
;;  (as-store [c] (->AsamiStore c nil (asami/graph (asami/db c)))))
