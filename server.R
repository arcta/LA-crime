
########################################################################
### www.arcta.me/rstudio/LA-crime/

library(shiny)
library("ggplot2")
library("RMySQL")

########################################################################

db = MySQL()
con = dbConnect(db, group = "rstudio")

########################################################################
# lat-lng filter here came from observation that
# archive seems cavering much wider area
# we want consistency in app, so, here is the filter
########################################################################

res = dbSendQuery(con, "SELECT date_format(date,'%m-%d') as date, year(date) as year,
    category, month(date) as month, weekday(date) as day, hour(date) as hour, city, lat, lng
    from crime WHERE lat > 33.50 and lat < 34.50 and lng > -119 and lng < -117.50
    HAVING year >= year(curdate())-5")
history = fetch(res, n = -1)
dbClearResult(res)

########################################################################

shinyServer(function(input, output) {

    filter = reactive({
        data = history
        if ("all" != input$category) {
            data = subset(data, category == input$category)
        }
        if ("all" != input$city) {
            data = subset(data, city == input$city)
        }
        data
    })

    output$timeProfile <- renderPlot({
        data = filter()
        if (dim(data)[1] > 0) {
            p = qplot(x = hour, data = data, colour = factor(year), geom = 'density')
            p + labs(title = "Time Profile")
        }
    })

    output$monthProfile <- renderPlot({
        data = filter()
        if (dim(data)[1] > 0) {
            p = qplot(x = month, data = data, colour = factor(year), geom = 'density')
            p + labs(title = "Seasonal Profile")
        }
    })

    output$message <- renderText({
        data = filter()
        if (dim(data)[1] > 0) {
            ""
        } else {
            "No incidents found ..."
        }
    })
})
