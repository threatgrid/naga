# Naga [![Build Status](https://travis-ci.org/threatgrid/naga.svg?branch=master)](https://travis-ci.org/threatgrid/naga)

Datalog based rules engine.

[![Clojars Project](http://clojars.org/org.clojars.quoll/naga/latest-version.svg)](http://clojars.org/org.clojars.quoll/naga)

Naga allows users to load data, and define rules to entailed new data. Once rules have been
executed, the database will be populated with new inferences which can be queried.

Naga can use its internal database, or wrap an external graph database. A command line
utility to demonstrate Naga will load rules, into memory, run them, and print all the
inferred results.

## Usage

To run Naga on a Datalog script, provide the script via stdin or in a filename argument.

While Naga is still in development, the easiest way to run it is with the
[Leiningen](http://leiningen.org) build tool.

```bash
  lein run pabu/family.lg
```

This runs the program found in the `family.lg` file. It loads the data specified in the file,
executes the rules, and finally prints the final results database, without the input data.

### Options

- `--init` Initialization data for the configured storage.
- `--json` Input path/url for JSON to be loaded and processed.
- `--out` Output file when processing JSON data (ignored when JSON not used).
- `--uri` URI describing a database to connect to. (default: `mem`. Datomic supported).

## Programs

The language being implemented is called "Pabu" and it strongly resembles Prolog. It is
simply a series of statements, which are of two types: assertions and rules. 

### Assertions
To declare facts, specify a unary or binary predicate.

```
  man(fred).
  friend(fred,barney).
```

The first statement declares that `fred` is of type `man`. The second declares that `fred` has
a `friend` who is `barney`.

Nothing needs to be declared about `man` or `friend`. The system doesn't actually care what
they mean, just that they can be used in this way. The use of these predicates is all the
declaration that they need.

### Rules
Rules are declared in 2 parts.

*head* :- *body* .


The *body* of the rule, defines the data that causes the rule
to be executed. This is a comma separated series of predicates, each typically containing
one or more variables. The predicate itself can also be variable
(this is very unusual in logic systems).

The *head* of the rule uses some of the variables in the *body* to declare new information
that the rule will create. It is comprised of a single predicate.

*Variables* are words that begin with a capital letter (yes, Prolog really does look like this).

Here is a rule that will infer an `uncle` relationship from existing data:

```
  uncle(Nibling,Uncle) :- parent(Nibling,Parent), brother(Parent,Uncle).
```

Note: *Nibling* is a word to describe children of your sibling, declared by linguist [Samuel E. Martin](https://en.wiktionary.org/wiki/nibling) in 1951. Maybe one of these days will make it into Merriam Webster.

In the above statement, *Nibling*, *Parent*, and *Uncle* are all variables. Once variables
have been found to match the predicates after the **:-** symbol, then they can be substituted
into the `uncle` predicate in the head of the rule.

### Other Syntax
Both assertions and rules end with a period.

Pabu (and Prolog) uses "C" style comments:

```
  /* This is a comment */
```

Any element can be given a namespace by using a colon separator. Only 1 colon may appear in an identifier.

```
  owl:SymmetricProperty(sibling).
```

To see this in use, look in pabu/family-2nd-ord.lg, and try running it:

```bash
  lein run pabu/family-2nd-ord.lg
```

## APIs

Naga defines a data access API to talk to storage. This is a Clojure protocol or Java interface
called `Storage`, found in `naga.store`. It should be possible to wrap most graph database APIs
in the `Storage` API.

For the moment, the only configured implementation is an in-memory store.

## In Memory Database

Naga implements its own graph database. It has a relatively capable query planner, and internal
operations for inner joins and projection. More operations are in the works.

Queries may be executed directly against the database, but for the moment they require API access.
This uses the same `Storage` API described above.

We also have some partial implementations for on-disk storage, which we hope to use.
These are based on the same architecture as the indexes in the
[Mulgara Database](http://github.com/quoll/mulgara).

This may eventually be split out into its own project.

## License

Copyright © 2016-2017 Cisco Systems

Copyright © 2011-2016 Paula Gearon

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
