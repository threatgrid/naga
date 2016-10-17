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
            [naga.data :as data]
            [naga.storage.memory.core])
  (:import [java.io File]))

(def stores (map name (keys @store/registered-stores)))

(def valid-output? #(.exists (.getParentFile (.getAbsoluteFile (File. %)))))
(def valid-input? #(.exists (File. %)))

(def cli-options
  [[nil "--storage STRING" "Select store type"
    :validate [(set stores) "Must be a registered storage type."]]
   [nil "--json STRING" "Filename for input json"
    :validate [valid-input? "Input file does not exist."]]
   [nil "--output STRING" "Filename for output json"
    :validate [valid-output? "Output directory does not exist."]]
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
  [in-stream]
  (let [{:keys [input output stats]} (run-all in-stream)]
      (println "INPUT DATA") 
      (doseq [a input] (println (predicate-string a)))
      (println "\nNEW DATA")
      (doseq [a output] (println (predicate-string a)))))

(defn json-program
  "Runs a program over data in a JSON file"
  [in-stream json-file out-file storage]
  (let [; TODO: handle storage URIs to determine type and connection
        ; fresh-store (instantiate-storage storage)
        fresh-store (store/get-storage-handle {:type :memory})
        {:keys [rules axioms]} (pabu/read-stream in-stream)

        json (data/stream->triples fresh-store json-file)

        basic-store (store/assert-data fresh-store axioms)
        json-data (data/stream->triples basic-store json-file)
        loaded-store (store/assert-data basic-store json-data)

        config {:type :memory ; TODO: type from flag+URI (store-type storage)
                :store loaded-store}

        program (r/create-program rules [])

        ;; run the program
        [store stats] (e/run config program)
        output (data/store->str store)]
    (spit out-file output)))

(defn -main [& args]
  (let [{{:keys [halp json output storage]} :options,
         arguments :arguments :as opts} (parse-opts args cli-options)]

    (when halp (exit 1 (usage opts)))
    (with-open [in-stream (if-let [filename (first arguments)]
                            (io/input-stream filename)
                            *in*)]
      (if json
        (json-program in-stream json output storage)
        (logic-program in-stream)))))
