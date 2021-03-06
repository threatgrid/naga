(defproject org.clojars.quoll/naga-cli "0.3.16-SNAPSHOT"
  :description "Forward Chaining Rule Engine CLI"
  :url "http://github.com/threatgrid/naga"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojars.quoll/naga "0.3.15"]
                 [org.clojure/clojure "1.10.3"]
                 [prismatic/schema "1.1.12"]
                 [org.clojure/tools.cli "0.3.5"]
                 [org.clojure/core.cache "1.0.207"]
                 [org.clojars.quoll/parsatron "0.0.10"]
                 [cheshire "5.10.0"]
                 [org.clojars.quoll/naga-store "0.5.2"]
                 [org.clojars.quoll/asami "2.0.2"]
                 ; [com.datomic/datomic-pro "0.9.5697" :exclusions [com.google.guava/guava] ; uncomment for Datomic Pro
                 [com.datomic/datomic-free "0.9.5697"]
                 [org.postgresql/postgresql "9.3-1102-jdbc41"]]
  :deploy-repositories {"releases" {:url "https://repo.clojars.org" :creds :gpg}}
  :profiles {
    :uberjar {:aot :all}
    :dev {:plugins [[lein-kibit "0.1.5"]]}}
  :scm {:dir ".."}
  :main naga.cli)
