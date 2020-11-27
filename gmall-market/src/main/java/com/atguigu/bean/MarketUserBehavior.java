package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class MarketUserBehavior {
    // 属性：用户ID，用户行为，推广渠道，时间戳
    private Long userId;
    private String behavior;
    private String channel;
    private Long timestamp;
}

