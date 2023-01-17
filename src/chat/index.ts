import React, { useState, useEffect } from 'react';
import { Chat, ChatList, ChatItem } from 'daisyui';

const data = [
  { author: 'terchris', message: "how are you?" },
  { author: 'ChatGPT', message: "I'm fine than you" },
  { author: 'terchris', message: "Are you a computer?" },
  { author: 'ChatGPT', message: "Yes" },
];

const ChatDialogue: React.FC = () => {
  const [selectedBubble, setSelectedBubble] = useState<number>(0);
  let intervalId = null;
  useEffect(() => {
    intervalId = setInterval(() => {
        setSelectedBubble(selectedBubble + 1);
    }, 2000);
    return () => clearInterval(intervalId);
  }, []);

  return (
    <Chat>
      <ChatList>
        {data.map((item, index) => (
          <ChatItem
            key={index}
            author={item.author}
            message={item.message}
            hidden={selectedBubble !== index}
          />
        ))}
      </ChatList>
    </Chat>
  );
};

export default ChatDialogue;
