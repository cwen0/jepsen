(ns tidb.register
  "Single atomic register test"
  (:refer-clojure :exclude [test])
  (:require [jepsen [client :as client]
             [checker :as checker]
             [generator :as gen]
             [independent :as independent]
             [util :refer [meh]]]
            [jepsen.checker.timeline :as timeline]
            [clojure.java.jdbc :as j]
            [clojure.tools.logging :refer :all]
            [tidb.sql :as c :refer :all]
            [tidb.basic :as basic]
            [knossos.model :as model]))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defrecord RegisterClient [conn]
  client/Client

  (open! [this test node]
    (assoc this :conn (c/open node)))

  (setup! [this test]
    (j/execute! conn ["drop table if exists test"])
    (j/execute! conn ["create table if not exists test
                      (id int primary key, val int)"]))

  (invoke! [this test op]
    (with-txn op [c conn]
      (try
        (let [id   (key (:value op))
              val' (val (:value op))
              val  (-> c (j/query [(str "select * from test where id = ? FOR UPDATE") id] :row-fn :val) first)]
          (case (:f op)
            :read (assoc op :type :ok, :value (independent/tuple id val))

            :write (do
                     (if (nil? val)
                       (j/insert! c :test {:id id :val val'})
                       (j/update! c :test {:val val'} ["id = ?" id]))
                     (assoc op :type :ok))

            :cas (let [[expected-val new-val] val'
                       cnt (j/update! c :test {:val new-val} ["id = ? and val = ?" id expected-val])]
                   (assoc op :type (if (zero? (first cnt))
                                     :fail
                                     :ok))))))))

  (teardown! [this test])

  (close! [this test]
    (c/close! conn)))

(defn register-test-base
  [opts]
  (basic/basic-test
   (merge
    {:client {:client (:client opts)
              :during (independent/concurrent-generator
                       10
                       (range)
                       (fn [k]
                         (->> (gen/reserve 5 (gen/mix [w cas cas]) r)
                              (gen/delay-til 1/2)
                              (gen/stagger 1/10)
                              (gen/limit 100))))}
     :checker (checker/compose
               {:perf (checker/perf)
                :indep (independent/checker
                        (checker/compose
                         {:timeline (timeline/html)
                          :linear (checker/linearizable
                                   {:model (:model opts)})}))})}
    (dissoc opts :client :during :model))))

(defn test
  [opts]
  (register-test-base
   (merge {:name "register"
           :client (RegisterClient. nil)
           :model (model/cas-register 0)}
          opts)))

(defrecord MultiRegisterClient [node]
  client/Client
  (setup! [this test node]
    (j/with-db-connection [c (conn-spec (first (:nodes test)))]
      (j/execute! c [str ("create table if not exists test "
                          " (id int primary key, val int, ik int)")]))
    (assoc this :node node))

  (invoke! [this test op])

  (teardown! [this test]))

; (defn multi-register-test
;   [opts]
;   (register-test-base
;    (merge {:name "multi-register"
;            :client (MultiRegisterClient. nil)
;            :model (multi-register {})}
;           opts)))

