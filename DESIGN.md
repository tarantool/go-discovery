# Cluster discovery Go library

## Оглавление

- [Обзор](#обзор)
  - [Архитектура](#архитектура)
- [Рассмотренные альтернативы](#рассмотренные-альтернативы)
- [Основная библиотека `go-discovery`](#основная-библиотека-go-discovery)
  - [Конфигурация узла](#конфигурация-узла)
  - [Фильтрация узлов](#фильтрация-узлов)
  - [Получение списка узлов](#получение-списка-узлов)
  - [Подписка на изменения конфигурации](#подписка-на-изменения-конфигурации)
- [Пул соединений `go-discovery/pool`](#пул-соединений-go-discoverypool)
  - [Пример настройки пула](#пример-настройки-пула)
- [Заключение](#заключение)

## Обзор

Клиенту нужно получать список узлов Tarantool из хранилища кластерной конфигурации, фильтровать их по тегам, ролям и режимам RW/RO, отслеживать изменения и отправлять запросы через пул соединений.

Проектные решения

- Реализовать discovery в отдельной библиотеке `go-discovery` на основе коннектора `go-tarantool`.
- Вынести пул соединений в подпакет `go-discovery/pool`.
- Применять аналогичный подход для других языков и коннекторов.
- На первом этапе использовать etcd как хранилище, в котором хранится и обновляется вся конфигурация кластера.
- Предусмотреть расширение для Tarantool Config Storage и сервиса discovery на базе Tarantool.

### Архитектура

![Схема взаимодействия клиента, go-discovery и пула соединений с etcd и Tarantool](docs/assets/cluster-discovery-architecture.png)

- Решение опирается на `go-tarantool/pool` и `tt/lib/cluster`.
- Клиент может получать топологию через `go-discovery` и создавать пул соединений через `go-discovery/pool`.
- Пул принимает все типы запросов из `go-tarantool`, включая запросы `go-tarantool/crud`.
- На схеме показаны etcd и источники, предусмотренные для дальнейшего расширения.

## Рассмотренные альтернативы

Остановились на отдельной библиотеке, которую можно использовать в языке Go. Для других коннекторов/языков при необходимости будет разработана похожая библиотека с похожим API. Поэтому я старался не использовать в API чего-то специфического для Go.

Но можно было пойти и другими путями.

### Создать отдельный сервис для autodiscovery

Выглядит так, что это лучше реализовывать на базе Tarantool. Т.е. чтобы каждый отдельный узел (либо с определенной ролью) знал о доступности других узлов. Вроде как в будущем это запланировано.

Отдельный микросервис на Go потребует больше работ и не входит в настоящие планы. Но в дальнейшем он может быть реализован на базе этой библиотеки.

### Сделать реализацию функционала в рамках коннектора go-tarantool

Тут несколько доводов против:

1. Предложенный функционал - это **EE**, закрытая часть. Коннектор `go-tarantool` сейчас открыт. Т.е. чисто технически нельзя сделать это частью коннектора.
2. Хотелось бы сохранить коннектор сравнительно небольшим. Во-первых, чтобы не подтягивать лишних зависимостей. Во-вторых, чтобы его было легче поддерживать, релизить и т.д. Независимо от других модулей.

## Основная библиотека `go-discovery`

### Конфигурация узла

Необходимо выделить минимальную информацию, которую будет предоставлять библиотека об узле, и при этом наиболее полезную в контексте задачи.

#### `Mode`

Вспомогательный тип - enumeration. Необходим чтобы задать `master`/`replica` режим работы для узла.

```go
// Mode - это режим работы узла.
type Mode int
const (
	// ModeAll - режим, при котором не важно master это или replica. В плане
	// конфигурации означает, что узел может быть и master, и replica.
	ModeAll Mode = iota
	// ModeRW - в узел можно писать и читать.
	ModeRW
	// ModeRO - с узла можно только читать.
	ModeRO
)
```

#### `Instance`

Конфигурация отдельного узла. Стоит отметить, что сейчас нельзя задавать отдельные теги для узлов (но это будет реализовано позже). На данный момент есть два варианта для размещения тегов:

1. [roles_cfg.tags](https://github.com/tarantool/tarantool/blob/f58bfc97c627f1838696697339fb3e443c05767a/src/box/lua/config/instance_config.lua#L2194-L2197)
2. [app.cfg.tags](https://github.com/tarantool/tarantool/blob/f58bfc97c627f1838696697339fb3e443c05767a/src/box/lua/config/instance_config.lua#L1637-L1643)

В дальнейшем способы задания тегов могут расширяться.

```go
// Instance описывает конфигурацию отдельного узла.
type Instance struct {
	// Group имя текущей группы узла.
	Group      string
	// Replicaset имя текущего replicaset узла.
	Replicaset string
	// Name имя текущего узла.
	Name       string
	// Mode режим работы текущего узла, rw/ro.
	Mode       Mode
	// URI список URI для подключения к узлу.
	URI        []string
	// Roles список ролей узла.
	Roles      []string
	// RoleTags список тегов ролей узла, может быть сконфигурирован
	// с помощью set строк в roles_cfg.tags.
	//
	// [https://github.com/tarantool/tarantool/blob/f58bfc97c627f1838696697339fb3e443c05767a/src/box/lua/config/instance_config.lua#L2194-L2197](https://github.com/tarantool/tarantool/blob/f58bfc97c627f1838696697339fb3e443c05767a/src/box/lua/config/instance_config.lua#L2194-L2197)
	RoleTags   []string
	// AppTags список тегов приложения, может быть сконфигурирован с
	// помощью set строк в app.cfg.tags.
	//
	// [https://github.com/tarantool/tarantool/blob/f58bfc97c627f1838696697339fb3e443c05767a/src/box/lua/config/instance_config.lua#L1637-L1643](https://github.com/tarantool/tarantool/blob/f58bfc97c627f1838696697339fb3e443c05767a/src/box/lua/config/instance_config.lua#L1637-L1643)
	AppTags    []string
	// Tags список тегов для узла.
	// Сейчас не реализованы теги для отдельных узлов в Tarantool. Будет
	// реализовано позже, тогда и будет возможность добавить этот функционал.
	// Tags       []string
}
```

### Фильтрация узлов

Согласно поставленной задачи, нам нужно отфильтровывать узлы по каким-то условиям. В качестве основы для условия выступает конфигурация отдельного узла. Я выделил отдельный общий интерфейс, и типы для базовых фильтров по отдельным полям. Но при желании этот список можно дополнить как на уровне библиотеки. Так и на уровне клиентского кода.

#### `Filter`

Filter - это базовый интерфейс для фильтрации узла по каким-то условиям.

```go
type Filter interface {
	// Filter возвращает true, если instance соответствует условиям.
	Filter(instance Instance) bool
}
```

#### `GroupsFilter`

GroupsFilter оставляет узлы только для заданных имен групп.

```go
type GroupsFilter struct {
	// Groups - список имен групп для фильтрации.
	Groups []string
}

// Все остальные фильтры тоже реализуют интерфейс Filter, опустил для того
// чтобы было меньше букв.
func (f GroupsFilter) Filter(instance Instance) bool {
	// ...
}
```

#### `ReplicasetsFilter`

ReplicasetsFilter оставляет узлы только для заданных имен репликасетов.

```go
type ReplicasetsFilter struct {
	// Replicasets - список имен репликасетов для фильтрации.
	Replicasets []string
}
```

#### `NamesFilter`

NamesFilter оставляет узлы только для заданных имен узлов.

```go
type NamesFilter struct {
	// Names - список имен узлов для фильтрации.
	Names []string
}
```

#### `ModeFilter`

ModeFilter оставляет узлы только для заданных режимов работы.

```go
type ModeFilter struct {
	// Mode - список режимов работы для фильтрации.
	Mode []Mode
}
```

#### `URIFilter`

`URIFilter` оставляет узлы только с заданными URI.

```go
type URIFilter struct {
	// URI - список URI для фильтрации.
	URI []string
}
```

#### `RolesFilter`

RolesFilter оставляет узлы только с заданными ролями.

```go
type RolesFilter struct {
	// Roles - список ролей для фильтрации.
	Roles []string
}
```

#### `RoleTagsFilter`

RoleTagsFilter оставляет узлы только с заданными тегами для ролей.

```go
type RoleTagsFilter struct {
	// RoleTags - список тегов ролей для фильтрации.
	RoleTags []string
}
```

#### `AppTagsFilter`

AppTagsFilter оставляет узлы только с заданными тегами для приложений.

```go
type AppTagsFilter struct {
	// AppTags - список тегов приложений для фильтрации.
	AppTags []string
}
```

#### Теги отдельных узлов

TagsFilter оставляет узлы только с заданными тегами.

Сейчас не реализованы теги для отдельных узлов в Tarantool. Будет реализовано позже, тогда и будет возможность добавить этот функционал.

```go
//type TagsFilter struct {
//	// Tags - список тегов для фильтрации.
//	Tags []string
//}
```

### Получение списка узлов

Мы должны уметь получать список конфигураций узлов из etcd, отфильтровать его по каким-то параметрам, узнать какие из узлов доступны для соединения. Тут пойдет речь о типах, которые предоставляют этот функционал.

В дальнейшем возможно расширение на Tarantool Config Storage и Tarantool.

#### `Discoverer`

Базовый интерфейс для получения списка узлов.

```go
// Discoverer - интерфейс, который реализует метод Discovery: получение
// конфигурации узлов.
type Discoverer interface {
	// Discovery вовзращает список конфигураций узлов, актуальный на момент
	// вызова.
	Discovery(ctx context.Context) ([]Instances, error)
}
```

#### `EtcdDiscoverer`

На данный момент нам нужна конкретная реализации этого интерфейса для получения списка конфигураций узлов из etcd.

```go
// EtcdDiscoverer нужен для получения списка конфигураций узлов из etcd.
type EtcdDiscoverer struct {
	// client - объект подключения к etcd из библиотеки etcd
	// https://pkg.go.dev/go.etcd.io/etcd/client
	// Соответственно, пользователь библиотеки сам конфигурирует соединение к
	// etcd.
	client *clientv3.Client
	// prefix - это префикс, по которому хранится конфигурация tarantool в
	// etcd.
	prefix string
}

// NewEtcdDiscoverer создает новый объект для получения списка узлов из
// etcd.
func NewEtcdDiscoverer(client *clientv3.Client, prefix string) *EtcdDiscoverer {
	// ...
}

// Discovery возвращает список конфигураций узлов, полученный из etcd.
func (d *EtcdDiscoverer) Discovery(ctx context.Context) ([]Instances, error) {
	// ...
}
```

#### `FilterDiscoverer`

Кроме того, было бы полезно реализовать на этом уровне функционал для
фильтрации списка узлов. Чтобы отфильтровать список заранее и выполнять, при возможности, меньше работы на следующих.

```go
// FilterDiscoverer нужен для фильтрации списка узлов по каким-то
// критериям.
type FilterDiscoverer struct {
	// discoverer - это базовая реализация Discoverer, результат выполнения
	// Discovery() которой уже фильтруется.
	discoverer Discoverer
	filters    []Filter
}

// NewFilterDiscoverer создает новый объект Discoverer для фильтрации списка
// поступающих узлов согласно заданным фильтрам.
func NewFilterDiscoverer(discoverer Discoverer,
	filters ...Filter) *FilterDiscoverer {
	// ...
}

// Discovery вызывает d.discoverer.Discovery() и возвращает отфильтрованный
// список узлов.
func (d *FilterDiscoverer) Discovery(ctx context.Context) ([]Instances, error) {
	// ...
}
```

#### `ConnectableDiscoverer`

Кроме того, с клиентской точки зрения было бы полезно добавить возможность
/получить список узлов, доступных для подключения в моменте. Для этого
можно завести дополнительный тип.

```go
// ConnectableDiscoverer возвращает список узлов, доступных для подключения.
type ConnectableDiscoverer struct {
	// factory помогает создать Dialer для инстанса, чтобы установить соединение.
	factory DialerFactory
	// discoverer - это базовая реализация Discoverer, результат выполнения
	// Discovery() которой уже фильтруется по возможности подключения.
	discoverer Discoverer
}

// NewConnectableDiscoverer возвращает новый объект ConnectableDiscoverer,
// который позволяет получить список доступных для соединения узлов.
func NewConnectableDiscoverer(factory DialerFactory, discoverer Discoverer) *ConnectableDiscoverer {
	// ...
}

// Discovery вызывает d.discoverer.Discovery() и возвращает список узлов,
// доступных для подключения.
func (d *ConnectableDiscoverer) Discovery(ctx context.Context) ([]instances, error) {
	// ...
}
```

#### Пример получения списка узлов

Тогда, получить список доступных для соединения узлов, отфильтрованный по каким-то параметрам, в клиентском коде можно будет получить так:

```go
// Клиентский код самостоятельно конфигурирует подключение к etcd.
etcdcli := client.New(client.Config{
	Endpoints: []string{"http://127.0.0.1:2379"},
})
defer etcdcli.Close()

discoverer := NewConnectableDiscoverer(
	NewFilterDiscoverer(
		NewEtcdDiscoverer(etcdcli, "/prefix"),
		RolesFilter{[]string{"vshard", "config"}},
		NamesFilter{[]string{"instance-001", "instance-002"}},
		ModeFilter{ModeRW}
	)
)
isnstances, err := discoverer.Discovery(ctx)
if err != nil {
	return err
}
```

#### Дополнительные источники конфигурации

В дальнейшем мы сможем добавить TcsDiscoverer и TarantoolDiscoverer для получения списка актуальных узлов из TCS (Tarantool Config Storage) и Tarantool (необходимо добавить функционал в Tarantool) соответственно.

### Подписка на изменения конфигурации

Кроме того, нам нужно иметь возможность поддерживать список узлов в актуальном состоянии. Для этого нужны дополнительные типы для возможности подписки на обновления.

#### `Event`

Единственный тип события. Можно было бы разделить на несколько, но вроде как достаточно и одного.

```go
// Event - базовая структура для события обновления списка узлов.
type Event struct {
	// Old - старая конфигурация узла, может отсутствовать, если узел
	// новый.
	Old Instance
	// HasOld - флаг, обозначающий присутствие старой конфигурации.
	HasOld bool
	// New - новая конфигурация узла, может отсутствовать, если узел
	// был удален.
	New Instance
	// HasNew - флаг, обозначающий присутствие новой конфигурации.
	HasNew bool
}
```

#### `Observer`

Кроме того, нам нужен интерфейс, который будет реализован получателем событий.

```go
// Observer интерфейс для получения новых событий.
type Observer interface {
	// Observe получает новые события или ошибку, после поступления ошибки новые
	// события приходить не будут.
	Observe(events []Event, err error)
}
```

#### `Scheduler`

Получать конфигурацию нужно с некоторой периодичностью. Ещё один интерфейс для этих целей.

```go
// Scheduler интерфейс, который позволяет запускать обновления по некоторому
// расписанию.
type Scheduler interface {
	// Wait позволяет ждать до следующего наступления события.
	Wait(ctx context.Context) error
	// Stop прекращает периодическую генерацию событий.
	Stop()
}
```

#### `PeriodicScheduler`

Конкретная реализация. Позволяет обновлять конфигурацию с некоторой периодичностью во времени.

```go
// PeriodicScheduler генерирует событие обновления с определенным периодом.
type PeriodicScheduler struct {
	period time.Duration
}

// NewPeriodicScheduler возвращает новый объект типа PeriodicScheduler.
func NewPeriodicScheduler(period time.Duration) *PeriodicScheduler {
	// ...
}

// Wait позволяет ожидать следующего периода.
func (s *PeriodicScheduler) Wait(ctx context.Context) error {
	// ...
}

// Stop прекращает периодическую генерацию событий.
func (s *PeriodicScheduler) Stop() {
	// ...
}
```

#### `EtcdWatchScheduler`

etcd позволяет подписаться на обновления по некоторому префиксу. А значит, мы можем создавать события при изменении ключей по этому префиксу.

```go
// EtcdWatchScheduler генерирует событие обновления при обновлении конфигурации
// по префиксу в etcd.
type EtcdWatchScheduler struct {
	// client - объект подключения к etcd из библиотеки etcd
	// https://pkg.go.dev/go.etcd.io/etcd/client
	// Соответственно, пользователь библиотеки сам конфигурирует соединение к
	// etcd.
	client *clientv3.Client
	// prefix - это префикс, по которому хранится конфигурация tarantool в
	// etcd.
	prefix string
}

// NewEtcdWatchScheduler возвращает новый объект EtcdWatchScheduler.
func NewEtcdWatchScheduler(client *clientv3.Client,
	prefix string) *EtcdWatchScheduler {
	// ...
}

// Wait позволяет ожидать пока не обновится конфигурация в etcd.
func (s *EtcdWatchScheduler) Wait(ctx context.Context) error {
	// ...
}

// Stop прекращает слежение за префиксом и генерации событий.
func (s *EtcdWatchScheduler) Stop() {
	// ...
}
```

#### Дополнительные источники событий

В будущем можно будет реализовать TcsWatchSubscriber и TarantoolWatchSubscriber для отслеживания обновлений в Tarantool Config Storage и Tarantool соответственно.

#### `Subscriber`

Кроме того, нужен интерфейс, который будет реализован конкретными типами, предоставляющим подписку на обновления.

```go
// Subscriber интерфейс, который должен быть реализован типом, предоставляющий
// обновления актуального списка узлов.
type Subscriber interface {
	// Subscribe позволяет подписаться на обновления.
	Subscribe(observer Observer) error
	// Unsubscribe позволяет перестать получать обновления.
	Unsubscribe(observer Observer)
}
```

#### `ScheduleSubscriber`

ScheduleSubscriber по расписанию Scheduler получает список узлов от Discoverer, сранивает его с текущем и посылает обновления подписчику.

```go
// ScheduleSubscriber позволяет совместить Scheduler и Discoverer.
// Он получает список узлов от Discoverer по заданному расписанию и
// шлёт обновления подписчику.
type ScheduleSubscriber struct {
	scheduler  Scheduler
	discoverer Discoverer
}

// NewScheduleSubscriber создаёт новый объект SchedulerSubscriber для
// получения списка актуальных узлов.
func NewScheduleSubscriber(scheduler Scheduler,
	discoverer Discoverer) ScheduleSubscriber {
	// ...
}

// Subscribe позволяет подписаться на обновления.
func (s *ScheduleSubscriber) Subscribe(observer Observer) error {
	// ...
}

// Unsubscribe позволяет перестать получать обновления.
func (s *ScheduleSubscriber) Unsubscribe(observer Observer) {
	// ...
}
```

#### `FilterSubscriber`

Кроме того, мы должны уметь фильтровать список событий.

```go
// FilterSubscriber позволяет фильтровать поток событий от базового Subscriber
// и вычленять из него только узлы с подходящими конфигурациями.
type FilterSubscriber struct {
	subscriber Subscriber
	filters    []Filter
}

// NewFilterSubscriber создает новый объект для фильтрации списка событий.
func NewFilterSubscriber(subscriber Subscriber,
	filters ...Filter) (*FilterSubscriber, error) {
	// ...
}

// Subscribe позволяет подписаться на отфильтрованные обновления.
func (s *FilterSubscriber) Subscribe(observer Observer) error {
	// ...
}

// Unsubscribe позволяет перестать получать обновления.
func (s *FilterSubscriber) Unsubscribe(observer Observer) {
	// ...
}
```

#### `ConnectableSubscriber`

Ну и мы должны уметь получать информацию о событиях только для подключенных узлов. Генерировать дополнительные события при их отключении.

```go
// ConnectableSubscriber позволяет фильтровать поток событий от базового
// Subscriber и вычленять из него только узлы доступные для подключения.
// Кроме того, генерируют дополнительные событие при отключении узла.
type ConnectableSubscriber struct {
	subscriber Subscriber
}

// Subscribe позволяет подписаться на обновления доступности узлов для
// подключения.
func (s *ConnectableSubscriber) Subscribe(observer Observer) error {
	// ...
}

// Unsubscribe позволяет перестать получать обновления.
func (s *ConnectableSubscriber) Unsubscribe(observer Observer) {
	// ...
}
```

#### Пример подписки на изменения

Пример кода для обновления списка узлов из etcd, доступных для подключения и отфильтрованных по каким-то параметрам:

```go
// Реализация Observer в клиентском коде.
type someObserver struct {
	wg sync.WaitGroup
}

func (o someObserver) Observe(events []Event, err error) {
	if err != nil {
		fmt.Printf("error: %s", err)
		wg.Done()
	}
	for event, _ := range events {
		switch{
		case event.HasNew && !event.HasOld:
			fmt.Printf("instance added: %q", event.New)
		case !event.HasNew && event.HasOld:
			fmt.Printf("instance removed: %q", event.Old)
		case event.HasNew && event.HasOld:
			fmt.Printf("instance updated, old: %q, new: %q", event.Old, event.New)
		}
	}
}

// Создаем клиент для etcd в клиенстком коде.
etcdcli := client.New(client.Config{
	Endpoints: []string{"[http://127.0.0.1:2379](http://127.0.0.1:2379/)"},
})
defer etcdcli.Close()

// Создаем Discoverer из etcd с какими-то фильтрами.
discoverer := NewFilterDiscoverer(
	NewEtcdDiscoverer(etcdcli, "/prefix"),
	RolesFilter{[]string{"vshard", "config"}},
	NamesFilter{[]string{"instance-001", "instance-002"}},
	ModeFilter{ModeRW}
)

// Создаем Scheduler по обновлению ключей в etcd для заданного префикса.
scheduler := NewEtcdWatchScheduler(etcdcli, "/prefix")
defer scheduler.Stop()

// Создаём Subscriber, который бы получал обновления из etcd, фильтровал
// бы узлы по каким-то признакам и выдавал только узлы доступные для
// подключения.
subscriber := NewConnectableSubscriber(
	NewFilterSubscriber(
		NewScheduleSubscriber(scheduler, discoverer),
		AppTagsFilter{[]string{"app-1", "app-2"}}
	)
)

// Создаём свой объект someObserver.
var observer someObserver.
observer.wg.Add(1)

// Подписываемся на обновления:
subscriber.Subscribe(observer)
defer subscriber.Unsubscribe(observer)

// Ждём первую ошибку и завершаем работу.
observer.wg.Wait()
```

## Пул соединений `go-discovery/pool`

Базовую библиотеку можно использовать для усовершенствования уже имеющегося пула соединений `go-tarantool/pool`. Он представляет собой сравнительно простой пул соединений, в который можно добавлять новые соединения, убирать ненужные, и может следить за доступностью отдельных соединений. В целом, то что нам и нужно.

На его базе можно построить более умный pool соединений с autodiscovery.

### `DialerFactory`

Вспомогательный тип, который создает объект `tarantool.Dialer` для конкретного соединения. Необходим для того, чтобы была возможность установить разные параметры подключения для разных узлов.

```go
type DialerFactory interface {
	NewDialer(instance Instance) (tarantool.Dialer, error)
}
```

### `NetDialerFactory`

Базовая реализация фабрики, которая не использует TLS. При желании можно будет расширить другими реализациями (в том числе и для TLS, и для разных узлов задавать разные параметры и т.д.).

```go
// NetDialerFactory создаёт объекты для создания сетевых подключений для
// заданного узла.
type NetDialerFactory struct {
	// Username - имя пользователя для подключений.
	Username string
	// Password - пароль для подключений.
	Password string
}

// NewDialer создаёт новый объект tarantool.Dialer для заданного узла.
func (f NetDialerFactory) NewDialer(instance Instance) (tarantool.Dialer, error) {
	// ...
}
```

### `Balancer`

Пользователям хотелось бы иметь разные стратегии балансировки запросов. Для этого нам нужно задать базовый интерфейс балансировщика.

> **Замечание по дизайну.** Эффективность интерфейса балансировщика и его реализаций нужно оценить на бенчмарках. Интерфейс может измениться, сохранив поддержку сценария ниже.

> Из шести инстансов три находятся в локальном дата-центре и три в удалённом. Запросы идут по round-robin между доступными локальными инстансами. Если локальных инстансов нет, запросы уходят в удалённый дата-центр.

Этот сценарий можно реализовать с помощью `PriorityBalancer`.

```go
// Balancer интерфейс, который должен быть реализован балансировщиков запросов,
// позволяет выбрать следующий узел для запроса.
type Balancer interface {
	// Add добавляет узел в список активных для балансировщика.
	Add(instance Instance)
	// Remove удаляет узел из списка активных для балансировщика.
	Remove(instance name)
	// Next получает имя следующего активного узла куда нужно слать запросы.
	// Второе возвращаемое значение true, если следующий узел существует.
	Next(mode Mode) (string, bool)
}
```

### `RoundRobinBalancer`

Потокобезопасный балансировщик отправляет запросы по очереди на все узлы.

```go
// RoundRobinBalancer - отсылает запросы на все узлы по-очереди.
type RoundRobinBalancer struct {
}

// NewRoundRobinBalancer создаёт новый балансировщик в режиме round-robin.
func NewRoundRobinBalancer() *RoundRobinBalancer {
	// ..
}

// Add добавляет узел в список активных узлов для балансировщика.
func (b *RoundRobinBalancer) Add(instance Instance) {
	// ..
}

// Remove удаляет узел из списка активных узлов балансировщика.
func (b *RoundRobinBalancer) Remove(name string) {
	// ..
}

// Next возвращает следующий узел для выполнения запроса. Следующий узел
// выбирается по-очереди из всех узлов с заданным режимом.
func (b *RoundRobinBalancer) Next(mode Mode) (string, bool) {
	// ..
}
```

### `PriorityBalancer`

Балансировщик с приоритетом. На узлах с наибольшим приоритетом выполняются запросы, если приоритеты узлов равны, то запросы на них выполняются в round-robin.

```go
// PriorityBalancer позволяет выполнять запросы только на определенном узле или
// группе узлов, для которых установлен высший приоритет.
type PriorityBalancer struct {
	// ..
}

// NewPriorityBalancer создаёт балансировщик на основе функции оценки веса приоритета
// узла.
func NewPriorityBalancer(priorityFunc func(instance Instance) int) *PriorityBalancer {
	// ..
}

// Add добавляет узел в список активных узлов для балансировщика.
func (b *PriorityBalancer) Add(instance Instance) {
	// ..
}

// Remove удаляет узел из списка активных узлов балансировщика.
func (b *PriorityBalancer) Remove(name string) {
	// ..
}

// Next возвращает следующий узел для выполнения запроса. Следующий узел
// выбирается по-очереди из всех узлов с заданным режимом.
func (b *PriorityBalancer) Next(mode Mode) (string, bool) {
	// ..
}
```

### `Pool`

Pool соединений с autodiscovery. Для подписки на изменения используется объект Subscriber из базовой библиотеки. Опции для подключения к узлам задаются при создании пула соединений.

```go
// Pool соединений, следит за доступностью, выполняет запросы только на
// доступных узлов в Round Robin.
type Pool struct {
	// В основе реализации лежит pool соединений из go-tarantool.
	pool       tarantoolpool.Pool
	subscriber Subscriber
	balancer   Balancer
	factory    DialerFactory
	opts       tarantool.Opts
}

// NewPool возвращает новые объект Pool. Который подписывается на обновления
// соединений из Subscriber, использует для отправки запросов заданный
// балансировщик и подключается к узлам с заданными опциями.
func NewPool(subscriber Subscriber, balancer Balancer,
	factory DialerFactory, opts tarantool.Opts) (Pool, error) {
	// ...
}

// Do выполняет запрос асинхронно. Логика работы такая же как у методов
// Connection/Pool из go-tarantool. Используются запросы из go-tarantool,
// а значит они могут быть переиспользованы все.
func (p *Pool) Do(request tarantool.Request) (*tarantool.Future) {
	// ...
}

// DoMode выполняет запрос асинхронно для узлов с заданным режимом работы
// (rw/ro/all). Нам нужен этот метод потому что в etcd не всегда хранится
// информация о текущем режиме работы узла. В случае off/manual настройки
// replication.failover хранится. В случае же election и supervised
// информация о текущем режиме работы узла доступна только при подключения
// к узлу из box.info.ro.
//
// Соответственно, заранее отфильтровать все узлы по режиму работы не получится
// в общем случае (только для off/manual failover), а значит Pool должен
// будет дополнительно опрашивать узлы с неизвестным режимом работы
// и читать box.info.ro.
func (p *Pool) DoMode(request tarantool.Request, mode Mode) (*tarantool.Future) {
	// ...
}

// Close завершает работу пула соединений.
func (p *Pool) Close() {
	// ...
}
```

### `ModePool`

Адаптер, который позволяет из обычного Pool сделать объект, реализующий интерфейс `tarantool.Doer` для выбранного режима работы узлов. Нужно это для того, чтобы не создавать разные пулы для только `rw`/`ro` узлов, что было бы неэффективно (нужно было бы 2 пула, а значит опрашивать в случае `election` или `supervised` failover в два раза больше узлов. Для `off` и `manual` failover можно это сделать с помощью фильтров без дополнительных накладных расходов).

```go
type ModePool struct {
	// Pool - базовый pool соединений.
	pool *Pool
	// Mode это выбранный режим работы.
	mode Mode
}

// NewModePool возвращает новый объект с заданным пулом соединений и режимом.
func NewModePool(pool *Pool, mode Mode) *ModePool {
	// ..
}

// Do выполняет запрос с выбранным режимом работы узлов для пула соединений.
func Do(request tarantool.Request) (*tarantool.Future) {
	return pool.DoMode(pool, mode)
}
```

### Пример настройки пула

```go
// Создаём клиент etcd по некоторым параметрам.
etcdcli := client.New(client.Config{
	Endpoints: []string{"[http://127.0.0.1:2379](http://127.0.0.1:2379/)"},
})
defer etcdcli.Close()

// Создаем Discoverer из etcd с какими-то фильтрами.
discoverer := NewFilterDiscoverer(
	NewEtcdDiscoverer(etcdcli, "/prefix"),
	RolesFilter{[]string{"vshard", "config"}},
	NamesFilter{[]string{"instance-001", "instance-002"}},
	ModeFilter{ModeRW}
)

// Создаем Scheduler по обновлению ключей в etcd.
scheduler := NewEtcdWatchScheduler(etcdcli, "/prefix")
defer scheduler.Stop()

// Создаём Subscriber, который бы получал обновления из etcd, фильтровал
// бы узлы по каким-то признакам. На основе этой, и предыдущей фильтрации
// будут фактически отфильтрованы узлы используемые в pool.
subscriber := NewConnectableSubscriber(
	NewFilterSubscriber(
		NewScheduleSubscriber(scheduler, discoverer),
		AppTagsFilter{[]string{"app-1", "app-2"}}
	)
)

// Создаём балансировщик, который отдаёт предпочтение узлам из определенных
// датацентров.
balancer := NewPriorityBalancer(func(instance Instance) int {
	switch instance.Group {
	case "datacenter-1":
		return 20
	case "datacenter-2":
		return 10
	default:
		return 0
	}
})

// Создаём новый пул соединений. Подключаемся для пользователя guest, 5 секунд
// таймаут на отдельный запрос.
factory := NetDialerFactory{
	Username: "guest",
}
pool, _ := NewPool(subscriber, balancer,
	factory, tarantool.Opts{Timeout: 5*time.Second})
defer pool.Close()

// Создаём объект для удобной отправки запросов в RW узлы.
rwpool := NewModePool(pool, ModeRW)
// Создаём объект для удобной отправки запросов в RO узлы.
ropool := NewModePool(pool, ModeRO)

// Выполняем запрос из go-tarantool на RW узлах. Аналогично бы он выполнялся
// в go-tarantool.Connection.
data, err := rwpool.Do(tarantool.NewSelectRequest("foo").Key([]any{"bar"}).Get()
fmt.Println(data)
fmt.Println(err)

// Выполняем запрос из go-tarantool на RO узлах. Аналогично бы он выполнялся
// в go-tarantool.Connection.
data, err := ropool.Do(tarantool.NewSelectRequest("foo").Key([]any{"bar"}).Get()
fmt.Println(data)
fmt.Println(err)
```
