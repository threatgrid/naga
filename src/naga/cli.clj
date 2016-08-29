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

(defn run-all
  "Runs a program, and returns the data processed, the results, and the stats.
   Takes an input stream. Returns a map of:
  :input, :output, :stats"
  [in]
        ;; read the program
  (let [{:keys [rules axioms]} (pabu/read-stream in)

        ;; instantiate a database. For the demo we're using "in-memory"
        fresh-store (store/get-storage-handle {:type :memory})

        ;; assert the initial axioms. The program can do that, but
        ;; we want to do it here so we can see the original store
        ;; (we may use it for comparisons some time in the future)
        original-store (store/assert-data fresh-store axioms)

        ;; Configure the storage the program will use. Provide a store
        ;; so the program won't try to create its own
        config {:type :memory :store original-store}

        ;; compile the program
        program (r/create-program rules [])

        ;; run the program
        [store results] (e/run config program)

        ;; dump the database by resolving an unbound constraint
        data (store/resolve-pattern store '[?e ?p ?v])]
    {:input axioms
     :output (remove (set axioms) data)
     :stats results}))


(defn- nm
  "Returns a string version of a keyword. These are not being represented
   as Clojure keywords, so namespaces (when they exist) are separated by
   a : character"
  [k]
  (if-let [n (namespace k)]
    (str n ":" (name k))
    (name k)))


(defn- predicate-string
  "Convert a predicate triplet into a string."
  [[e p v]]
  (if (= p :rdf/type)
    (str (nm e) "(" (nm v) ").")
    (str (nm p) "(" (nm e) ", " (nm v) ").")))


(defn -main [& args]
  (let [{:keys [options arguments] :as opts} (parse-opts args cli-options)]
    (when (:halp options) (exit 1 (usage opts)))
    (let [in-stream (if-let [filename (first arguments)]
                      (io/input-stream filename)
                      *in*)
          {:keys [input output stats]} (run-all in-stream)]
      (println "INPUT DATA") 
      (doseq [a input] (println (predicate-string a)))
      (println "\nNEW DATA")
      (doseq [a output] (println (predicate-string a))))))
