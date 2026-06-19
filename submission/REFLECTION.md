# Reflection — Day 17 (≤ 200 từ)

1. **Flywheel.** Bước dễ hỏng nhất là `build_preference_pairs` — nếu logic ghép prompt→chosen/rejected sai, bạn lặng lẽ tạo cặp sai mà không có lỗi. Phát hiện bằng cách kiểm tra cardinality của prompt: mỗi prompt unique phải có đúng 1 ok và 1 error turn. Giám sát tỷ lệ raw pairs / clean pairs sau decontamination.

2. **Decontamination.** Bỏ qua bước này đồng nghĩa với việc train trên chính prompt trong eval set, làm sai lệch độ chính xác eval. Hậu quả thể hiện khi prompt thật (không trong eval) cho kết quả kém hơn benchmark — contamination kinh điển.

3. **Point-in-time.** Trong phát hiện gian lận, join tổng số giao dịch của user *tính đến hôm nay* vào feature của giao dịch quá khứ làm rò rỉ dữ liệu tương lai. Không có `ASOF`, model học pattern không tồn tại tại thời điểm inference.

4. **Graph vs vector.** KG trả lời "Kho nào gửi widget?" (2-hop) trong khi vector search thất bại vì không chunk nào chứa cả hai thực thể. Vector là thừa cho "Chính sách đổi trả widget?" — một chunk là đủ, embedding chỉ thêm độ trễ.