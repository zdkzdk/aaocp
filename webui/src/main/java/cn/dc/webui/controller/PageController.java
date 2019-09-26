package cn.dc.webui.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class PageController {
    @RequestMapping("show")
    public String catelogAdd(){
        return "show";
    }
}
