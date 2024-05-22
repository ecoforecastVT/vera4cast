# ```{r}
# if("IceCover_binary_max" %in% unique(df$variable)){
#
#   ggobj_df <- df |>
#   filter(variable == c("IceCover_binary_max")) |>
#     mutate(observation = as.numeric(NA))
#
# if(nrow(ggobj_df) > 0){
#
# ggobj <- ggobj_df |>
# ggplot(aes(x = datetime, y = mean, color = model_id)) +
#   geom_line_interactive(aes(datetime, mean, col = model_id,
#                               tooltip = model_id, data_id = model_id),
#                         show.legend=FALSE) +
#   facet_wrap(~site_id) +
#   ylim(0,1) +
#   labs(y = "Predicted probability") +
#   theme_bw()
#
# girafe(ggobj = ggobj,
#          width_svg = 8, height_svg = 4,
#          options = list(
#            opts_hover_inv(css = "opacity:0.20;"),
#            opts_hover(css = "stroke-width:2;"),
#            opts_zoom(max = 4)
#          ))
# }
# }
#```
