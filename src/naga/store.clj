(ns ^{:doc "Storage API for talking to external storage"
      :author "Paula Gearon"}
    naga.store)

(defprotocol Storage
  (start-tx [store] "Starts a transaction, if supported")
  (commit-tx [store] "Commits a transaction, if supported")
  (resolve-pattern [store pattern] "Resolves a pattern against storage")
  (count-pattern [store pattern] "Counts the size of a pattern resolition against storage")
  (query [store output-pattern patterns] "Resolves a set of patterns (if not already resolved) and joins the results")
  (assert-data [store data] "Inserts new axioms")
  (query-insert [store assertion-pattern patterns] "Resolves a set of patterns, joins them, and inserts the set of resolutions"))

(def registered-stores (atom {}))

(defn register-storage!
  "Registers a new storage type"
  [store-id factory-fn]
  (swap! registered-stores assoc store-id factory-fn))

(defn get-storage-handle
  "Creates a store of the configured type. Throws an exception for unknown types."
  [{type :type store :store :as config}]
  (or store
      (if-let [factory (@registered-stores type)]
        (factory config)
        (throw (ex-info "Unknown storage configuration" config)))))
