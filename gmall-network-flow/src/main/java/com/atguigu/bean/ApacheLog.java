package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ApacheLog {

    private String ip;
    private String userId;
    private Long eventTime;
    private String method;
    private String url;

}
