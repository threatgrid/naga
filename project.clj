(defproject org.clojars.quoll/naga "0.2.28"
  :description "Forward Chaining Rule Engine"
  :url "http://github.com/threatgrid/naga"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/clojurescript "1.10.597"]
                 [prismatic/schema "1.1.10"]
                 [org.clojure/tools.cli "0.3.5"]
                 [org.clojure/core.cache "0.7.1"]
                 [org.clojars.quoll/parsatron "0.0.9"]
                 [cheshire "5.8.0"]
                 [org.clojars.quoll/naga-store "0.3.3"]
                 [org.clojars.quoll/asami "0.4.1"]
                 [org.clojars.quoll/qtest "0.1.1"]
                 ; [com.datomic/datomic-pro "0.9.5697" :exclusions [com.google.guava/guava] ; uncomment for Datomic Pro
                 [com.datomic/datomic-free "0.9.5697" :exclusions [com.google.guava/guava]]
                 [org.postgresql/postgresql "9.3-1102-jdbc41"]]
  :plugins [[lein-cljsbuild "1.1.7"]]
  :profiles {:dev {:plugins [[lein-kibit "0.1.5"]]}}
  :cljsbuild {
    :builds {
        :dev {
          :source-paths ["src"]
          :compiler {
            :output-to "target/js/main.js"
            :optimizations :simple
            :pretty-print true}}
        :test {
          :source-paths ["src" "test"]
          :compiler {
            :output-to "target/js/test.js"
            :optimizations :simple
            :pretty-print true}}}
    :test-commands {
      "unit" ["node" "target/js/test.js"]}}
  :main naga.cli)
