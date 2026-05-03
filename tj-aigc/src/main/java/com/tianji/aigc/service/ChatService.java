package com.tianji.aigc.service;


import com.tianji.aigc.vo.ChatEventVO;
import reactor.core.publisher.Flux;

public interface ChatService  {

    Flux<ChatEventVO> chat(String question, String sessionId);
}
