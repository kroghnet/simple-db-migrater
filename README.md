# simple-db-migrater

This is a simple database migrater tool, which was created because I find Flyway too big for simple forward DB migration.

Migration is done by reading either SQL files from a folder, or SQL resources somewhere on the classpath, and applying those files in the natural sort order, of their names.
A table for containing information about already applied scripts is created, and will contain a record for each applied sql file. Files will only be applied once.
All upgrades will be applied in a single transaction, ensuring that the database cannot be left in an undefined state.

## SQL files
Each SQL file is considered a migration step, but can contain any number of SQL statements, separated by semicolon (;). The SQL files may contain comments (currently only line-comments, beginning with //), and the individual statements may be split over multiple lines, for readbility,

## Folder or resource path
In order to allow upgrade scripts to be delivered either by operations or by developers - or both, SQL files may be placed in a folder outside the application, or embedded as resources in a jar file.

