(ns ^{:doc "Command line interface for interacting with Naga."
      :author "Paula Gearon"}
    naga.cli
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.string :as string]
            [clojure.java.io :as io]
            [naga.lang.pabu :as pabu]
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

(defn -main [& args]
  (let [{:keys [options arguments] :as opts} (parse-opts args cli-options)]
    (when (:halp options) (exit 1 (usage opts)))
    (let [input (if-let [filename (first arguments)]
                  (io/input-stream filename)
                  *in*)
          program (pabu/read-stream input)]
      (clojure.pprint/pprint program))))

