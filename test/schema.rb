ActiveRecord::Schema.define(:version => 1) do
  create_table :test_models, :force => true do |t|
    t.column :title, :string
    t.column :description, :string
    t.column :created_at, :timestamp
  end
end