package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ChannelBehaviorCount {
    private String channel;
    private String behavior;
    private String windowEnd;
    private Long count;
}
