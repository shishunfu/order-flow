# 架构概览

第一阶段采用模块化单体，单个 Spring Boot 部署单元内包含 order 与 inventory 两个业务模块。

- Order 负责订单生命周期及订单数据。
- Inventory 负责库存数据与扣减规则。
- Order 通过 InventoryApplicationService 请求库存能力。
- 各模块拥有自己的 Repository 接口和基础设施实现。

订单创建在单库事务中执行：查询幂等键 → 条件扣减库存 → 插入订单。后续引入 MQ 时，需要重新设计事件发布和一致性边界。
