(ns naga.engine
  (:require [naga.structs :as st :refer [EPVPattern RulePatternPair Body Program]]
            [naga.queue :as q]
            [naga.store :as store]
            [naga.util :as u]
            [schema.core :as s])
  (:import [naga.structs Rule]
           [naga.store Storage]))

(def true* (constantly true))

(s/defn execute
  "Executes a program"
  [rules :- {s/Str Rule}
   db-store :- Storage]
  (let [rule-queue (reduce q/add
                           (q/new-queue :salience :name)
                           (vals rules))]
    (loop [queue rule-queue storage db-store]
        (let [{:keys [status body head downstream execution-count] :as current-rule} (q/head queue)
              remaining-queue (q/pop queue)]

          (if (nil? current-rule)
            ;; finished, build results as rule names mapped to how often the rule was run
            (u/mapmap :name (comp deref :execution-count) (vals rules))

            ;; find if any patterns have updated
            (if-let [dirty-patterns (seq (keep (fn [[p status-atom]] (if (some-> status-atom deref :dirty) p)) status))]
              ;; rule needs to be run
              (let [resolved-patterns (keep (fn [p]
                                              (let [resolution (store/resolve storage p)
                                                    last-count (:last-count @(status p))]
                                                (when-not (= last-count (count resolution))
                                                  (with-meta p {:resolution resolution}))))
                                            dirty-patterns)
                    resolved-set (into #{} resolved-patterns)
                    hinted-patterns (map #(get resolved-set % %) body)]  ;; hinted patterns have resolutions for meta

                ;; mark the rule as cleaned with its latest count
                (doseq [dp dirty-patterns :let [
                                              ]]
                  (let [{r :resolution} (if-let [rp (get resolved-set dp)] (meta rp))
                        pattern-status (status dp)]
                    (reset! pattern-status {:last-count (if r
                                                          (count r)
                                                          (-> pattern-status deref :last-count))
                                            :dirty false})))

                ;; is there a NEW result to be had?
                (if (seq resolved-patterns)
                  ;; TODO: EXECUTE ACTIONS FOR ACTION RULES
                  ;; (if (= rule-type :action)
                  ;;   (do-action (store/query storage head hinted-patterns))
                  ;;   :else ...)
                  ;; insert data according to the rule
                  
                  (let [updated-storage (store/query-insert storage head hinted-patterns)

                        ;; schedule downstream
                        scheduled-queue
                        (reduce (fn [rqueue [rname pattern]]
                                  (let [{status :status :as sched-rule} (rules rname)
                                        constraint-data (get status pattern)]
                                    (assert constraint-data
                                            (str "rule-constraint pair missing in rule: " rname))
                                    (swap! constraint-data update-in [:dirty] true*)
                                    (q/add rqueue identity sched-rule)))
                                remaining-queue
                                downstream)] ;; downstream contains rule-name/pattern pairs for update
                    (swap! execution-count inc)
                    (recur scheduled-queue updated-storage))

                  ;; no new results, so move to next
                  (recur remaining-queue storage)))

              ;; no dirty patterns, so rule did not need to be run
              (recur remaining-queue storage)))))))

(s/defn run :- s/Bool
  "Runs a program against a given configuration"
  [config :- {s/Keyword s/Any}
   {:keys [rules axioms]} :- Program]
  (let [storage (store/get-storage-handle config)]
    (store/assert-data storage axioms)
    (execute rules storage)))
