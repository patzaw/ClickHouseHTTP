###############################################################################@
## Regenerate everything derived from the ClickHouseHTTP hex logo                      ##
###############################################################################@
##
## Source of truth: supp/logo/ClickHouseHTTP-hex-logo.odp
##   -> export it as supp/logo/ClickHouseHTTP-hex-logo.png (keep that exact name), then
##      run this script from the package root.
##
## It rebuilds, in order:
##   1. man/figures/logo.png        (README header + pkgdown navbar)
##   2. pkgdown/favicon/*           (requires internet)
##   3. README.md / README.html     (README.html embeds the logo as base64)
##   4. docs/*                      (the committed pkgdown site)
##
## `supp/` is listed in .Rbuildignore, so this script never ships with the
## package.
##
###############################################################################@

library(magick)
library(pkgdown)
library(rmarkdown)

stopifnot(file.exists("DESCRIPTION"))  # must be run from the package root

SOURCE_LOGO <- "supp/logo/ClickHouseHTTP-hex-logo.png"
stopifnot(file.exists(SOURCE_LOGO))

###############################################################################@
## 1. man/figures/logo.png ----
##
## `usethis::use_logo()` is the usual way to do this, but it asks for
## confirmation before overwriting an existing logo and therefore silently does
## nothing when run non-interactively (Rscript, knitr, CI). The two lines below
## are exactly what it does internally: resize to 240 px wide, keeping the
## aspect ratio of the source.
###############################################################################@

image_write(
  image_resize(image_read(SOURCE_LOGO), geometry_size_pixels(width = 240)),
  "man/figures/logo.png"
)

## No edit to README.Rmd is needed: it already points at man/figures/logo.png.

###############################################################################@
## 2. Favicons ----
##
## Sends man/figures/logo.png to realfavicongenerator.net (pkgdown's own
## mechanism) and rewrites all of pkgdown/favicon/. Needs internet access.
## site.webmanifest comes back with empty name/short_name fields; that is the
## expected output, leave it as is.
###############################################################################@

build_favicons(overwrite = TRUE)

# ###############################################################################@
# ## 3. README ----
# ##
# ## Watch the pandoc version: README.html embeds images as base64 and different
# ## pandoc releases emit different <img> markup (e.g. pandoc < 3.10 drops the
# ## role="img" aria-label="..." accessibility attributes). Rendering with a
# ## different pandoc than the previous build produces a large diff that has
# ## nothing to do with the logo.
# ##
# ## Set RSTUDIO_PANDOC to the *directory* holding the pandoc binary that was
# ## used for the previous build; check docs/pkgdown.yml ("pandoc:" field) to see
# ## which version that was. Adjust the path below for your machine.
# ###############################################################################@

# PANDOC_DIR <- "/home/system_folders/opt/quarto/bin/tools/x86_64"  # pandoc 3.10
# if (dir.exists(PANDOC_DIR)) {
#   Sys.setenv(RSTUDIO_PANDOC = PANDOC_DIR)
# }
# message("Rendering README with pandoc ", as.character(pandoc_version()))

# library(dplyr)  # README.Rmd's setup chunk calls filter()
# render("README.Rmd", quiet = TRUE)

# ###############################################################################@
# ## 4. docs/ ----
# ##
# ## Do NOT call pkgdown::build_site() here: it rebuilds the articles, and the
# ## vignettes need a live ClickHouse instance. The article HTML is pre-built and
# ## copied verbatim from pkgdown/assets/, so only the home page and the site
# ## assets need refreshing.
# ###############################################################################@

# init_site()    # copies logo.png and pkgdown/favicon/* into docs/
# build_home()   # regenerates docs/index.html from README.md

# ## build_reference() would refresh this one, but it runs the examples, so just
# ## copy the file across.
# file.copy("man/figures/logo.png", "docs/reference/figures/logo.png",
#           overwrite = TRUE)

# ###############################################################################@
# ## 5. Checks ----
# ###############################################################################@

# ## Same logo everywhere
# stopifnot(identical(
#   tools::md5sum("man/figures/logo.png")[[1]],
#   tools::md5sum("docs/logo.png")[[1]]
# ))
# stopifnot(identical(
#   tools::md5sum("man/figures/logo.png")[[1]],
#   tools::md5sum("docs/reference/figures/logo.png")[[1]]
# ))

# ## Favicons copied to the site
# for (f in list.files("pkgdown/favicon")) {
#   stopifnot(identical(
#     tools::md5sum(file.path("pkgdown/favicon", f))[[1]],
#     tools::md5sum(file.path("docs", f))[[1]]
#   ))
# }

# message("Done. Now review `git diff --stat` and check that:")
# message(" - README.md is unchanged (only README.html should move)")
# message(" - docs/index.html shows the new logo and favicon (hard-refresh)")
# message(" - `R CMD build .` still succeeds")
