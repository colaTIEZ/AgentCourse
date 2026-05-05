package com.tianji.aigc.service.impl;

import cn.hutool.core.date.DateUtil;
import com.tianji.aigc.config.SystemPromptConfig;
import com.tianji.aigc.enums.ChatEventTypeEnum;
import com.tianji.aigc.service.ChatService;
import com.tianji.aigc.vo.ChatEventVO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.memory.ChatMemory;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
@Slf4j
@RequiredArgsConstructor
public class ChatServiceImpl implements ChatService {

    private final ChatClient chatClient;
    private final SystemPromptConfig systemPromptConfig;
    private final ChatMemory chatMemory;
    private final StringRedisTemplate stringRedisTemplate;

    // 生成状态标识
    private static final String GENERATE_STATUS_KEY = "GENERATE_STATUS";


    @Override
    public Flux<ChatEventVO> chat(String question, String sessionId) {
        var conversationId = ChatService.getConversationId(sessionId);
        // 大模型输出内容的缓存器，用于在输出中断后的数据存储
        var outputBuilder = new StringBuilder();
        var hashOps = stringRedisTemplate.boundHashOps(GENERATE_STATUS_KEY);
        return chatClient.prompt()
                .system(promptSystem -> promptSystem
                        .text(this.systemPromptConfig.getChatSystemMessage().get())
                        .params(Map.of("now", DateUtil.now()))
                )
                .advisors(advisor -> advisor.param(ChatMemory.CONVERSATION_ID, conversationId))
                .user(question)
                .stream()
                .chatResponse()
                .doFirst(() -> hashOps.put(sessionId, "true")) // 第一次输出内容时执行
                .doOnError(throwable -> hashOps.delete(sessionId))// 出现异常时，删除标识
                .doOnComplete(() -> hashOps.delete(sessionId))// 完成时执行，删除标识
                .doOnCancel(() -> {
                    saveStopHistoryRecord(conversationId, outputBuilder.toString());
                })
                .takeWhile(response ->  // 通过返回值来控制Flux流是否继续，true：继续，false：终止
                        hashOps.get(sessionId) != null
                )
                .map(chatResponse -> {
                    String text = chatResponse.getResult().getOutput().getText();
                    // 追加到输出内容中
                    outputBuilder.append(text);
                    return ChatEventVO.builder()
                            .eventData(text)
                            .eventType(ChatEventTypeEnum.DATA.getValue())
                            .build();
                })
                .concatWith(Flux.just(ChatEventVO.builder()
                        .eventType(ChatEventTypeEnum.STOP.getValue())
                        .build()));//结束标识
    }

    /**
     * 保存停止输出的记录
     *
     * @param conversationId 会话id
     * @param content        大模型输出的内容
     */
    private void saveStopHistoryRecord(String conversationId, String content) {
        this.chatMemory.add(conversationId, new AssistantMessage(content));
    }

    @Override
    public void stop(String sessionId) {
        //移除标记
        var hashOps = stringRedisTemplate.boundHashOps(GENERATE_STATUS_KEY);
        hashOps.delete(sessionId);
    }
}
