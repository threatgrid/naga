(ns ^{:doc "Command line interface for interacting with Naga."
      :author "Paula Gearon"}
    naga.cli
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.string :as string]
            [clojure.java.io :as io]
            [naga.lang.pabu :as pabu]
            [naga.rules :as r]
            [naga.engine :as e]
            [naga.store :as store]
            [naga.storage.memory.core]))

(def stores (map name (keys @store/registered-stores)))

(def cli-options
  [["-s" "--storage" "Select store type"
    :validate [(set stores) "Must be a registered storage type."]]
   ["-h" "--halp" "Print help"]])

(defn exit
  [status message]
  (println message)
  (System/exit status))

(defn usage
  [{summary :summary}]
  (->> ["Executes Naga on a program."
        ""
        "Usage: naga [filename]"
        ""
        summary
        (str "Store types: " (into [] stores))
        ""]
       (string/join \newline)))

(defn nm [k]
  (if-let [n (namespace k)]
    (str n ":" (name k))
    (name k)))

(defn predicate-string
  [[e p v]]
  (str (nm p) "(" (nm e) ", " (nm v) ")."))

(defn -main [& args]
  (let [{:keys [options arguments] :as opts} (parse-opts args cli-options)]
    (when (:halp options) (exit 1 (usage opts)))
    (let [input (if-let [filename (first arguments)]
                  (io/input-stream filename)
                  *in*)
          {:keys [rules axioms]} (pabu/read-stream input)
          fresh-store (store/get-storage-handle {:type :memory})
          original-store (store/assert-data fresh-store axioms)
          config {:type :memory :store original-store}
          program (r/create-program rules [])
          [store results] (e/run config program)
          data (store/resolve-pattern store '[?e ?p ?v])]
      (println "INPUT DATA") 
      (doseq [a axioms] (println (predicate-string a)))
      (println "\nOUTPUT DATA")
      (doseq [a data] (println (predicate-string a))))))
