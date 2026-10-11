# 插件自动化 Action

插件可把一个有版本、类型和风险标记的 Action 注册进 Yuki 的持久化 Automation DSL。需要 `automation.action.register`。

```python
from yuki_plugin_sdk.models import PermissionLevel, RiskClass, StrictModel
from yuki_plugin_sdk.registrar import (
    AutomationActionMetadata,
    AutomationActionRegistration,
)


class GreetingInput(StrictModel):
    name: str


class GreetingOutput(StrictModel):
    text: str


async def greeting(arguments: GreetingInput) -> GreetingOutput:
    return GreetingOutput(text=f"记得喝水，{arguments.name}。")


registrar.register_automation_action(
    AutomationActionRegistration(
        metadata=AutomationActionMetadata(
            name="make_greeting",
            description="生成确定性提醒文本",
            permission=PermissionLevel.USER,
            risk=RiskClass.GENERATE,
            schema_version=1,
        ),
        input_model=GreetingInput,
        output_model=GreetingOutput,
        handler=greeting,
    )
)
```

`AutomationActionMetadata` 默认只允许 `scheduled_automation` 和 `system_task` 来源。普通用户可使用 `permission=user` 的 Action，但只能在自己的 Automation 和创建时允许的当前真实场景内；插件不能通过 Action 提升用户权限。

## 委托与兼容

创建任务时 Host 保存最小 `DelegatedAuthority` 和 Action Schema 版本。每次执行重新取交集：

```text
创建时授予 ∩ 当前插件批准 ∩ 当前 Action/Schema ∩ 当前用户权限
```

插件被禁用/删除、批准撤销或 Schema 版本变化时，依赖 Action 的任务进入 `blocked`，不会用新代码猜测执行旧参数。升级 Action 时保持旧 Schema，或提供显式迁移并提升版本。

Action 必须返回结构化结果；主动发送应作为单独受权步骤，不要在“生成文本”Action 中偷偷调用 OneBot。

原Run固定既存version唯一引用、当次delegation和已绑定canonical目标；新定义用于新Run，
不会因改名或相同hash的新委托覆盖旧执行。输入在派发前只做一次native解析，typed handler接收
原模型实例。未声明timeout时没有默认10秒门；作者明确期限和实际外部请求/取消仍生效。

普通PluginResult.data不承担Host控制、预算或失败分类。真实SDK Work/Social沿Host私有类型化
结果接回原CapabilityResult；waiting/suspended保原工作，查询不重新执行Action或转计旧累计。
SELF仅沿原自身主体、delegation和当前群范围执行获准公共读取，不借真人身份或开放私人资料。
SDK保留角色不被降成USER；没有真实入站来源的自动化仍不支持伪造真人调用SDK Agent生成。

