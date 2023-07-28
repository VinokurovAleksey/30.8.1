package storage

import (
	"context"

	"github.com/jackc/pgx/v4/pgxpool"
)

// Хранилище данных.
type Storage struct {
	db *pgxpool.Pool
}

// Конструктор, принимает строку подключения к БД.
func New(constr string) (*Storage, error) {
	db, err := pgxpool.Connect(context.Background(), constr)
	if err != nil {
		return nil, err
	}
	s := Storage{
		db: db,
	}
	return &s, nil
}

// Задача.
type Task struct {
	ID         int
	Opened     int64
	Closed     int64
	AuthorID   int
	AssignedID int
	Title      string
	Content    string
}

// Tasks возвращает список задач из БД.
func (s *Storage) Tasks(taskID, authorID int) ([]Task, error) {
	rows, err := s.db.Query(context.Background(), `
		SELECT 
			id,
			opened,
			closed,
			author_id,
			assigned_id,
			title,
			content
		FROM tasks
		WHERE
			($1 = 0 OR id = $1) AND
			($2 = 0 OR author_id = $2)
		ORDER BY id;
	`,
		taskID,
		authorID,
	)
	if err != nil {
		return nil, err
	}
	var tasks []Task
	// итерирование по результату выполнения запроса
	// и сканирование каждой строки в переменную
	for rows.Next() {
		var t Task
		err = rows.Scan(
			&t.ID,
			&t.Opened,
			&t.Closed,
			&t.AuthorID,
			&t.AssignedID,
			&t.Title,
			&t.Content,
		)
		if err != nil {
			return nil, err
		}
		// добавление переменной в массив результатов
		tasks = append(tasks, t)

	}
	// ВАЖНО не забыть проверить rows.Err()
	return tasks, rows.Err()
}

// NewTask создаёт новую задачу и возвращает её id.
func (s *Storage) NewTask(t Task) (int, error) {
	var id int
	err := s.db.QueryRow(context.Background(), `
		INSERT INTO tasks (title, content)
		VALUES ($1, $2) RETURNING id;
		`,
		t.Title,
		t.Content,
	).Scan(&id)
	return id, err
}

// TasksByAuthor возвращает список задач, созданных указанным автором.
func (s *Storage) TasksByAuthor(authorID int) ([]Task, error) {
	rows, err := s.db.Query(context.Background(), `
		SELECT
			id,
			opened,
			closed,
			author_id,
			assigned_id,
			title,
			content
		FROM tasks
		WHERE author_id = $1
		ORDER BY id;
`, authorID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []Task
	for rows.Next() {
		var t Task
		err = rows.Scan(
			&t.ID,
			&t.Opened,
			&t.Closed,
			&t.AuthorID,
			&t.AssignedID,
			&t.Title,
			&t.Content,
		)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, t)
	}

	return tasks, rows.Err()
}

// TasksByLabel возвращает список задач с указанной меткой.
func (s *Storage) TasksByLabel(label string) ([]Task, error) {
	rows, err := s.db.Query(context.Background(), `
		SELECT
			t.id,
			t.opened,
			t.closed,
			t.author_id,
			t.assigned_id,
			t.title,
			t.content
		FROM tasks t
		INNER JOIN task_labels tl ON t.id = tl.task_id
		INNER JOIN labels l ON tl.label_id = l.id
		WHERE l.label = $1
		ORDER BY t.id;
`, label)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []Task
	for rows.Next() {
		var t Task
		err = rows.Scan(
			&t.ID,
			&t.Opened,
			&t.Closed,
			&t.AuthorID,
			&t.AssignedID,
			&t.Title,
			&t.Content,
		)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, t)
	}

	return tasks, rows.Err()
}

// GetTaskByID возвращает задачу по её id.
func (s *Storage) GetTaskByID(id int) (Task, error) {
	var t Task
	err := s.db.QueryRow(context.Background(), `
		SELECT
			id,
			opened,
			closed,
			author_id,
			assigned_id,
			title,
			content
		FROM tasks
		WHERE id = $1;
`, id).Scan(
		&t.ID,
		&t.Opened,
		&t.Closed,
		&t.AuthorID,
		&t.AssignedID,
		&t.Title,
		&t.Content,
	)
	if err != nil {
		return Task{}, err
	}

	return t, nil
}

// UpdateTask обновляет задачу по её id.
func (s *Storage) UpdateTask(id int, updatedTask Task) error {
	_, err := s.db.Exec(context.Background(), `
		UPDATE tasks
		SET
			opened = $1,
			closed = $2,
			author_id = $3,
			assigned_id = $4,
			title = $5,
			content = $6
		WHERE id = $7;
`,
		updatedTask.Opened,
		updatedTask.Closed,
		updatedTask.AuthorID,
		updatedTask.AssignedID,
		updatedTask.Title,
		updatedTask.Content,
		id,
	)
	if err != nil {
		return err
	}

	return nil
}

// DeleteTaskByID удаляет задачу по её id.
func (s *Storage) DeleteTaskByID(id int) error {
	_, err := s.db.Exec(context.Background(), `
		DELETE FROM tasks WHERE id = $1;
`, id)
	if err != nil {
		return err
	}

	return nil
}
