(ns ^{:doc "Storage API for talking to external storage"
      :author "Paula Gearon"}
    naga.store)

(defprotocol Storage
  (resolve-pattern [store pattern] "Resolves a pattern against storage")
  (join [store output-pattern patterns] "Resolves a set of patterns (if not already resolved) and joins the results")
  (assert-data [store data] "Inserts new axioms")
  (query-insert [store assertion-pattern patterns] "Resolves a set of patterns, joins them, and inserts the set of resolutions"))

(defn get-storage-handle [config] {})
