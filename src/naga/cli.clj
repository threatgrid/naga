(ns ^{:doc "Command line interface for interacting with Naga."
      :author "Paula Gearon"}
    naga.cli
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.string :as string]
            [clojure.java.io :as io]
            [naga.lang.pabu :as pabu]
            [naga.store-registry :as store-registry]
            [naga.store :as store]
            [naga.rules :as r]
            [naga.engine :as e]
            [zuko.entity.reader :as data-reader]
            [zuko.entity.writer :as data-writer]
            [asami.core]
            [naga.storage.datomic.core])
  (:import [clojure.lang ExceptionInfo]
           [java.net URI]
           [java.io File]))

(def stores (set (map name (keys @store-registry/registered-stores))))

(def valid-output? #(.exists (.getParentFile (.getAbsoluteFile (File. %)))))
(def valid-input? #(.exists (File. %)))

(def as-uri #(if (instance? URI %) % (try (URI. %) (catch Exception _))))

(def cli-options
  [[nil "--storage STRING" "Select store type"
    :validate [stores "Must be a registered storage type."]]
   [nil "--uri STRING" "URI for storage"
    :parse-fn as-uri
    :validate [identity "Invalid storage URL"]]
   [nil "--init STRING" "Initialization data"
    :validate [valid-input? "Invalid initialization data file"]]
   [nil "--json STRING" "Filename for input json"
    :validate [valid-input? "Input file does not exist."]]
   [nil "--out STRING" "Filename for output json"
    :validate [valid-output? "Output directory does not exist."]]
   ["-h" "--halp" "Print help"]])

(defn exit
  [status & messages]
  (throw (ex-info (string/join messages) {:status status})))

(defn usage
  [{summary :summary}]
  (string/join
    \newline
    ["Executes Naga on a program."
     ""
     "Usage: naga [filename]"
     ""
     summary
     (str "Store types: " (vec stores))
     ""]))

(defn storage-configuration
  "Reads storage parameters, and builds an appropriate configuration structure"
  [{:keys [type uri init json]}]
  (let [uri (as-uri uri)
        store-from-uri (fn [u]
                         ;; may want this to be more complex in future
                         (when u
                           (let [s (.getScheme u)
                                 fs (and s (re-find #"[^:]*" s))]
                             (stores fs))))
        store-type (or type (store-from-uri uri))]
    (when (and uri (nil? store-type)) (exit 1 "Unable to determine storage type for: " uri))
    {:type (if store-type (keyword store-type) :memory)
     :uri uri
     :init init
     :json json}))

(defn run-all
  "Runs a program, and returns the data processed, the results, and the stats.
   Takes an input stream. Returns a map of:
  :input, :output, :stats"
  [in config]
        ;; read the program
  (let [{:keys [rules axioms]} (pabu/read-stream in)

        ;; instantiate a database. Config may include initialization data
        fresh-store (store-registry/get-storage-handle config)

        ;; assert the initial axioms. The program can do that, but
        ;; we want to do it here so we can see the original store
        ;; (we may use it for comparisons some time in the future)
        original-store (store/assert-data fresh-store axioms)

        ;; Configure the storage the program will use. Provide a store
        ;; so the program won't try to create its own
        config (assoc config :store original-store)

        ;; compile the program
        program (r/create-program rules [])

        ;; run the program
        [store stats] (e/run config program)

        ;; dump the database by resolving an unbound constraint
        data (store/resolve-pattern store '[?e ?p ?v])]
    {:input axioms
     :output (remove (set axioms) data)
     :stats stats}))

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
    (str (nm v) "(" (nm e) ").")
    (str (nm p) "(" (nm e) ", " (nm v) ").")))

(defn logic-program
  [in-stream config]
  (let [{:keys [input output stats]} (run-all in-stream config)]
      (println "INPUT DATA")
      (doseq [a input] (println (predicate-string a)))
      (println "\nNEW DATA")
      (doseq [a output] (println (predicate-string a)))))

(defn json-program
  "Runs a program over data in a JSON file"
  [in-stream json-file out-file store-config]
  (when-not out-file
    (exit 2 "No output json file specified"))
  (let [fresh-store (store-registry/get-storage-handle store-config)
        {:keys [rules axioms]} (pabu/read-stream in-stream)

        basic-store (store/assert-data fresh-store axioms)
        json-data (data-writer/stream->triples (:graph basic-store) json-file)
        loaded-store (store/assert-data basic-store json-data)

        config (assoc store-config :store loaded-store)

        program (r/create-program rules [])

        ;; run the program
        [store stats] (e/run config program)
        output (data-reader/graph->str (:graph store))]
    (spit out-file output)))

(defn -main [& args]
  (try
    (let [{{:keys [halp json out storage uri init] :as options} :options,
           arguments :arguments :as opts} (parse-opts args cli-options)
          storage-config (storage-configuration options)]

      (when halp (exit 1 (usage opts)))
      (with-open [in-stream (if-let [filename (first arguments)]
                              (io/input-stream filename)
                              *in*)]
        (if json
          (json-program in-stream json out storage-config)
          (logic-program in-stream storage-config))))
    (catch ExceptionInfo e
      (binding [*out* *err*]
        (println (.getMessage e))))
    (finally (store-registry/shutdown))))
