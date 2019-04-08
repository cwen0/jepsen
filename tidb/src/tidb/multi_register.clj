(ns tidb.multi-register
  (:refer-clojure :exclude [test read])
  (:require [jepsen [client :as client]
             [checker :as checker]
             [generator :as gen]
             [independent :as independent]
             [util :as util :refer [meh]]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.txn.micro-op :as mop]
            [jepsen.tests.linearizable-register :as lr]
            [clojure.java.jdbc :as j]
            [clojure.string :as str]
            [clojure.tools.logging :refer :all]
            [tidb.sql :as c :refer :all]
            [knossos.model :as model])
  (:import (knossos.model Model)))

; Three keys, five possible values per key.
(def key-range (vec (range 3)))
(defn rand-val [] (rand-int 5))

(defn r
"Read a random subset of keys."
[_ _]
(->> (util/random-nonempty-subset key-range)
     (mapv (fn [k] [:r k nil]))
     (array-map :type :invoke, :f :read, :value)))

(defn w [_ _]
"Write a random subset of keys."
(->> (util/random-nonempty-subset key-range)
     (mapv (fn [k] [:w k (rand-val)]))
     (array-map :type :invoke, :f :write, :value)))

(defrecord MultiRegisterClient [conn]
  client/Client

  (open! [this test node]
    (assoc this :conn (c/open node test)))

  (setup! [this test]
    (j/execute! conn ["create table if not exists test
                      (id int, val int, ik int, primary key(id, ik))"]))

  (invoke! [this test op]
    (c/with-error-handling op
      (let [[ik txn] (:value op)]
            (case (:f op)
              :read
              (let [ks   (vec (map mop/key txn))
                    vs   (->>(if (= txn nil)
                               (do (info "ik: " ik "ks: " ks "txn " txn)
                                 (j/query conn [(str "select * from test where ik = ? ")
                                              ik]))
                               (do (info (str "select * from test where ik = ? and id in (" (str/join "," (repeat (count ks) "?")) ")"))
                                   (info "ik: " ik "ks: " ks "txn " txn)
                                   (j/query conn [(str "select * from test where ik = ? and id in ( " (str/join "," (repeat (count ks) "?")) ") ")
                                                  ik ks])))
                             (map (juxt :id :val))
                             (into {}))
                    txn' (mapv (fn [[f k _]] [f k (get vs k)]) txn)]
                (assoc op :type :ok, :value (independent/tuple ik txn')))
              :write
              (j/with-db-transaction [c conn]
                (->> (for [[f k v] txn]
                       (do
                         (assert (= :w f))
                         (j/execute! c [(str "insert into test (id, ik, val) "
                                             "value (?, ?, ?) "
                                             "on duplicate key update val = ?")
                                        k ik v v])))
                     (assoc op :type :ok)))))))

  (teardown! [this test])

  (close! [this test]
    (c/close! conn)))

(defrecord MultiRegister []
  Model
  (step [this op]
    (reduce (fn [state [f k v]]
              ; Apply this particular op
              (case f
                :r (if (or (nil? v)
                           (= v (get state k)))
                     state
                     (reduced
                      (model/inconsistent
                       (str (pr-str (get state k)) "≠" (pr-str v)))))
                :w (assoc state k v)))
            this
            (:value op))))

(defn multi-register
  "A register supporting read and write transactions over registers identified
  by keys. Takes a map of initial keys to values. Supports a single :f for ops,
  :txn, whose value is a transaction: a sequence of [f k v] tuples, where :f is
  :read or :write, k is a key, and v is a value. Nil reads are always legal."
  [values]
  (map->MultiRegister values))

(defn workload
  [opts]
  (let [w (lr/test (assoc opts :model (multi-register {})))]
    (-> w
        (assoc :client (MultiRegisterClient. nil))
        (assoc :generator (let [n (count (:nodes opts))]
                          (independent/concurrent-generator
                           (* 2 n)
                           (range)
                           (fn [k]
                             (->> (gen/reserve n r w)
                                  (gen/stagger 1)
                                  (gen/process-limit 20)))))))))


