# QuantumDB

QuantumDB is a method for evolving database schemas of SQL databases, while not impeding access to SQL clients or
requiring downtime. It does this by making ghost tables for every original table that's under change by user-defined
changeset of schema operations. In order to ensure that every client has access to their specific version of the
database they can specify on which version of the schema they're relying, and the QuantumDB driver will do all the
work.

For more information, please checkout these links:

* [Overall presentation on QuantumDB / Zero-downtime database schema evolution](https://speakerdeck.com/michaeldejong/zero-downtime-database-schema-evolution)
* [My Msc Thesis on Zero-downtime database schema evolution](http://repository.tudelft.nl/assets/uuid:af89f8ba-fc34-4084-b479-154be397718f/thesis.pdf)

## Setup dev-environment

For most development work on QuantumDB you'll want to have a database server running to test and run against. For this
purpose this repository contains all you need to setup a VM which will run a recent version of PostgreSQL. Here's how
to get started:

```
gem install librarian-puppet
librarian-puppet install
vagrant up --provision
```

You should now be able to access PostgreSQL on port 5432 on localhost. Also note that puppet-librarian managed modules
should be excluded from Git file tracking. Only self-made modules should be tracked by Git.
