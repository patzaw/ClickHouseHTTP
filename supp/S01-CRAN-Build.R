library(here)
library(rmarkdown)

##############################@
## Build and copy vignettes ----
rmarkdown::render(here("README.Rmd"))

##############################@
## Build and check package ----
pv <- desc::desc_get_version(here())
system(paste(
   sprintf("cd %s", here("..")),
   "R CMD build ClickHouseHTTP",
   sprintf("R CMD check --as-cran ClickHouseHTTP_%s.tar.gz", pv),
   sep=" ; "
))
# install.packages(here(sprintf("../ClickHouseHTTP_%s.tar.gz", pv)), repos=NULL)
