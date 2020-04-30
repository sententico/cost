This is an (expanding) collection of Go packages supporting cloud services cost analysis.  Besides the importable packages are a number of command-line tools.

To use the packages in this module, add an import in your Go code and call it:

```
import "github.com/sententico/cost/csv

//...later
digest, err := csv.Peek(path)
```

The `cost/csv` package includes support for identifying and reading delimited-field CSV files (and fixed-field TXT files).  The `csv` command-line tool is a utility demonstrating use of this package and its ability to remember settings (like column maps) for known CSV file types.
