
########################################################################
### www.arcta.me/rstudio/LA-crime/

library(shiny)
library("RMySQL")

########################################################################

db = MySQL()
con = dbConnect(db, group = "rstudio")

res = dbSendQuery(con, "SELECT category FROM crime GROUP BY category")
category = fetch(res, n = -1)
dbClearResult(res)

res = dbSendQuery(con, "SELECT city FROM crime
WHERE lat > 33.50 and lat < 34.50 and lng > -119 and lng < -117.50
GROUP BY city")
city = fetch(res, n = -1)
dbClearResult(res)

########################################################################

shinyUI(fluidPage(
    fluidRow(
        column(1, helpText("")),

        column(3,
            h1("Rstudio:"),
            h3("LA Crime Statistics"),

            selectInput("category",
                label = "Incident Category:",
                choices = as.vector(rbind(c("all"), category)),
                selected = "criminal homicide"),

            selectInput("city",
                label = "City:",
                choices = as.vector(rbind(c("all"), city)),
                selected = "all"),

            textOutput("message")
        ),

        # Show density plots by year
        column(6,
            plotOutput("timeProfile"),
            plotOutput("monthProfile")
        ),

        column(2, helpText(""))
    )
))
