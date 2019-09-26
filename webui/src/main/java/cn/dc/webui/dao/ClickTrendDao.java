package cn.dc.webui.dao;

import cn.dc.webui.bean.ClickTrendBean;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

public interface ClickTrendDao extends JpaRepository<ClickTrendBean,Integer>, JpaSpecificationExecutor<ClickTrendBean> {
}

