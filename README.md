# Chainstdio

The basic idea of this project is to have the ability to index any blockchain locally to keep track of transactions made by any wallet without the need for a centralized service. 

The indexing process is based on three stages:
* Reconciliation, will scan the local information to check which ledgers are missing between the genesis block and the last stored, and save them before continuing.
* Synchronization, will search and store the blocks between the last one stored to the last one on the blockchain concurrently. 
* Polling, will keep fetching the latest mined block on the blockchain every N time.

## Progress
This is a quite fun pet project with a lot of missing features missings therefore is far from being code complete, however, the current features should be working:
* Reconciliation
* Synchronization

There are some missing changes to be made to the current features like:
* Use an embedded database instead a server-based one.
* Improve the storage model to avoid saving the same information and improve space and search velocity 
