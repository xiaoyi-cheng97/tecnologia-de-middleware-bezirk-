# TM
Projeto de Tecnologias de Middleware

This project had the objective of building a peer-to-peer network of nodes that gathered information from source nodes and then diffused that information to other nodes. Zookeeper was used to control the network topography by offering a ledger to node information (port and IP), but it has no central power nor decision making capabilities that influence the network topology (that would generate a Single Point of Failure in the Zookeeper infrastructure).
