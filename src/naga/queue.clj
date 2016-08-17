(ns naga.queue
  (:refer-clojure :exclude [pop])
  (:require [schema.core :as s :refer [=>]]))

(defprotocol PQueue
  (q-count [queue] "Returns the number of items in the queue")
  (head [queue] "Returns the head of the queue")
  (pop [queue] "Removes the head from the queue")
  (add
    [queue element]
    [queue update-fn element]
    "Adds an element to the queue if it isn't already present, returning the new queue. Uses update-fn on the element if it is already in the queue"))

(s/defn insert-by :- [s/Any]
  [s :- [s/Any]
   salience :- (=> s/Num s/Any)          ;; element -> number
   e :- s/Any]
  (let [preamble (if (and salience (salience e))
                   (take-while #(>= (salience e) (salience %)) s)
                   s)]
    (concat preamble
            (cons e
                  (drop (count preamble) s)))))

(defrecord SalienceQueue
  [q ;; :- [s/Any]
   h ;; :- #{s/Any}
   id-fn ;; :- (=> s/Any s/Any)  ;; element -> ID (id: string, number, etc)
   salience-fn ;; :- (=> s/Num s/Any)  ;; element -> number
  ]
  PQueue
  (q-count [_] (count q))
  (head [_] (first q))
  (pop [_] (->SalienceQueue (rest q) (disj h (id-fn (first q))) id-fn salience-fn))
  (add [e element] (add e identity element))
  (add
    [this update-fn element]
    (let [id (id-fn element)]
      (if (h id) ;; TODO: can salience be updated on the fly?
        (if (= identity update-fn)
          this  ;; shortcut to avoid redundant work when using identity
          (let [updater-fn (fn [e] (if (= id (id-fn e)) (update-fn e) e))]
            (->SalienceQueue (map updater-fn q) h id-fn salience-fn)))
        (->SalienceQueue (insert-by q salience-fn element)
                         (conj h id)
                         id-fn
                         salience-fn)))))

(s/defn new-queue
  "Create an empty queue. When called without arguments, salience is ignored,
   and update and ID are just identity."
  ([]
   (new-queue nil identity))
  ([salience-fn :- (=> s/Num s/Any)
    id-fn :- (=> s/Any s/Any)]
   (->SalienceQueue '() #{} id-fn salience-fn)))

(defn drain
  "Pulls everything off a queue into a seq."
  [queue]
  (loop [s [] q queue]
    (if-let [e (head q)]
      (recur (conj s e) (pop q))
      s)))
