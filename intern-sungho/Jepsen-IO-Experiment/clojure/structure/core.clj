(ns tutorial.core
  (:gen-class)) ; namespace


(defn -main [& args]

  (println "Define struct with variables")
  (defstruct Employee :EmployeeName :Employeeid)

  (println "\nInitialize an instance with the struct")
  (def emp (struct Employee "John" 1))
  (println emp)

  (println "\nInitialize an instance with struct-map")
  (def emp2 (struct-map Employee :EmployeeName "Chris" :Employeeid 2))
  (println emp2)

  (println "\nPrint variables from the instance")
  (println (:Employeeid emp))
  (println (:EmployeeName emp))

  (println "\ncopy a new instance with name Mark")
  (def newemp (assoc emp :EmployeeName "Mark"))
  (println newemp)

  (println "\ncopy a new instance with a rank A")
  (def newemp2 (assoc emp :EmployeeRank "A"))
  (println newemp2)

)


