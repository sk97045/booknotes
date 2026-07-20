
# Design Whatsapp

## 1. Requirements (~5 min)

![data-tables](images/hello-interview/1.png)
---

## 3. Core Entities

1. Users: UserId, Metadata
2. Chats (2-100 users): ChatId, Name, Metadata
3. ChatParticipient : ChatId (PK), ParticipientId (GSI)
4. Messages: MessageId, chatId, Contents, UserId, Timestamp
5. Clients (a user might have multiple devices): DeviceID, USerId, IP etc
6. Inbox (Undelivered Messages): UserId, MessageId

## 2. API Design

![data-tables](images/hello-interview/2.png)

## High-Level Design (~10–15 min)

### Arpit 
![data-tables](images/arpit/1.png)
![data-tables](images/arpit/2.png)

### ShowOffer
![data-tables](images/show-offer/1.png)
![data-tables](images/show-offer/2.png)
![data-tables](images/show-offer/3.png)


![data-tables](images/hello-interview/3.png)
---

## Deep Dives (~10 min)

## Best Design