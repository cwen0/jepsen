(ns tidb.multi-register
  (:refer-clojure :exclude [test])
  (:require [jepsen [client :as client]
             [checker :as checker]
             [generator :as gen]
             [independent :as independent]
             [util :refer [meh]]]
            [jepsen.checker.timeline :as timeline]
            [clojure.java.jdbc :as j]
            [clojure.tools.logging :refer :all]
            [tidb.sql :refer :all]
            [tidb.basic :as basic]
            [knossos.model :as model]))

(defrecord MultiRegisterClient [node]
  client/Client
  (setup! [this test node]
    (j/with-db-connection [c (conn-spec (first (:nodes test)))]
      (j/execute! c [str ("create table if not exists test "
                          " (id int primary key, val int, ik int)")]))
    (assoc this :node node))

  (invoke! [this test op])

  (teardown! [this test]))


(defn test
  [opts]
  (basic/basic-test
   (merge
    {:name "multi-register"
     :client {:client (MultiRegisterClient. nil)
              :during (independent/concurrent-generator
                       10
                       (range)
                       (fn [k]
                         (->> (gen/reserve 5 r w)
                              (gen/delay 1/2)
                              (gen/stagger 1/10)
                              (gen/limit 100))))}
     :model (multi-register {})
     :checker (checker/compose
               {:perf (checker/perf)
                :indep (independent/checker
                        (checker/compose
                         {:timeline (timeline/html)
                          :linear checker/linearizable}))})}
    opts)))
